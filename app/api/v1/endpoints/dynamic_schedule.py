import traceback
import re
from collections import defaultdict
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from pony.orm import db_session, select, ObjectNotFound, desc, commit
from pydantic import BaseModel

from app.models import Order, Operation, Machine, WorkCenter, PartScheduleStatus, PlannedScheduleItem, ScheduleVersion, \
    ProductionLog, ScheduleHistory, RescheduledItem, MachineStatus, Status
from app.crud.operation import fetch_operations
from app.crud.component_quantities import fetch_component_quantities
from app.crud.leadtime import fetch_lead_times
from app.algorithm.scheduling import schedule_operations, adjust_to_shift_hours, get_next_shift_start, get_shift_end
from app.schemas.scheduled1 import (
    RescheduledItemResponse,
    RescheduledItemList,
    ProductionLogResponse,
    CombinedScheduleResponse as LegacyCombinedScheduleResponse,
    WorkCenterInfo as LegacyWorkCenterInfo,
    MachineInfo as LegacyMachineInfo,
    ScheduledOperation,
    RescheduleUpdate,
)
from app.api.v1.endpoints.dynamic_algorithm import (
    get_production_logs_for_rescheduling,
    process_reschedule_triggers,
    reschedule_operation_with_cascade,
    is_default_machine
)

router = APIRouter(prefix="/api/v1/dynamic-schedule", tags=["dynamic-schedule"])


class DynamicScheduleRequest(BaseModel):
    """Request model for dynamic scheduling"""
    start_time: Optional[datetime] = None
    force_reschedule: bool = False


class DynamicScheduleResponse(BaseModel):
    """Response model for dynamic scheduling"""
    message: str
    rescheduled_items_count: int
    production_logs_processed: int
    reschedule_triggers: List[Dict[str, Any]]
    execution_time: float
    timestamp: datetime


class MachineInfo(BaseModel):
    id: int
    name: str


class WorkCenterInfo(BaseModel):
    id: int
    name: str
    machines: List[MachineInfo]


class RescheduleInfo(BaseModel):
    """Information about a reschedule trigger"""
    operation_id: int
    order_id: int
    machine_id: int
    production_order: str
    part_number: str
    completed_qty: int
    remaining_qty: int
    original_end_time: datetime
    production_log_end_time: datetime
    reschedule_reason: str


def get_active_parts_for_scheduling() -> Dict[Tuple[str, str], int]:
    """Get active parts and their quantities for scheduling"""
    try:
        with db_session:
            # Get active part statuses
            active_statuses = select(
                (ps.part_number, ps.production_order)
                for ps in PartScheduleStatus
                if ps.status == 'active'
            )[:]

            if not active_statuses:
                return {}

            # Get component quantities for active parts
            component_quantities = {}
            for part_number, production_order in active_statuses:
                try:
                    # Try to get quantity from orders
                    order = select(o for o in Order if o.production_order == production_order).first()
                    if order:
                        # Use required_quantity as the main quantity
                        component_quantities[(part_number, production_order)] = order.launched_quantity
                    else:
                        # Default quantity if order not found
                        component_quantities[(part_number, production_order)] = 1
                except Exception as e:
                    print(f"Error getting quantity for {part_number}-{production_order}: {e}")
                    component_quantities[(part_number, production_order)] = 1

            return component_quantities

    except Exception as e:
        print(f"Error getting active parts: {e}")
        return {}


def clear_rescheduled_items():
    """Clear existing rescheduled items"""
    try:
        with db_session:
            # Delete all existing rescheduled items
            select(r for r in RescheduledItem).delete(bulk=True)
            commit()
    except Exception as e:
        print(f"Error clearing rescheduled items: {e}")
        raise


def copy_scheduled_to_rescheduled_items() -> int:
    """Copy scheduled items to rescheduled items with same timestamps when no production logs exist"""
    try:
        with db_session:
            # Get all scheduled items
            scheduled_items = select(ri for ri in RescheduledItem if ri.status == 'scheduled')[:]

            if not scheduled_items:
                print("No scheduled items found to copy")
                return 0

            items_copied = 0

            for item in scheduled_items:
                try:
                    # Create a rescheduled item with the same timestamps
                    RescheduledItem(
                        order=item.order,
                        operation=item.operation,
                        machine=item.machine,
                        start_time=item.start_time,
                        end_time=item.end_time,
                        total_qty=item.total_qty,
                        completed_qty=0,  # No production logs, so completed_qty = 0
                        remaining_qty=item.total_qty,  # All quantity remaining
                        status='reschedule',  # Mark as rescheduled
                        type=item.type  # Keep same type (setup/cycle)
                    )
                    items_copied += 1
                    print(
                        f"Copied scheduled item: {item.operation.operation_description} ({item.type}) - {item.start_time} -> {item.end_time}")

                except Exception as e:
                    print(f"Error copying scheduled item {item.id}: {e}")
                    continue

            commit()
            print(f"Successfully copied {items_copied} scheduled items to rescheduled items")
            return items_copied

    except Exception as e:
        print(f"Error copying scheduled to rescheduled items: {e}")
        return 0


def purge_rescheduled_items_for_orders(order_ids: set, machine_id: Optional[int] = None):
    """Remove existing rescheduled items for specific orders (optionally scoped to a machine)."""
    if not order_ids:
        return
    try:
        with db_session:
            query = select(ri for ri in RescheduledItem if ri.status == 'reschedule' and ri.order.id in order_ids)
            if machine_id is not None:
                query = query.filter(lambda ri: ri.machine.id == machine_id)
            count = query.count()
            if count:
                print(
                    f"Purging {count} existing rescheduled items for orders {order_ids}{' on machine ' + str(machine_id) if machine_id is not None else ''}")
                query.delete(bulk=True)
                commit()
    except Exception as e:
        print(f"Error purging rescheduled items: {e}")
        raise


def reschedule_operations_without_logs(
        schedule_df: pd.DataFrame,
        production_logs: List[Dict],
        forced_start_time: Optional[datetime] = None,
        machine_ids: Optional[set] = None,
        order_ids_allowlist: Optional[set] = None,
        processed_keys: Optional[set] = None
) -> int:
    """
    Reschedule operations that don't have production logs but need to be rescheduled
    due to cascade effects from operations that do have logs.
    Optionally start from a forced anchor time.

    Args:
        schedule_df: Original schedule DataFrame
        production_logs: List of operations with production logs
        forced_start_time: If provided, use this as the initial cascade anchor time
        machine_ids: Optional set of machine IDs to filter by
        order_ids_allowlist: Optional set of order IDs to filter by
        processed_keys: Set of (operation_id, order_id) that have already been processed

    Returns:
        int: Number of operations rescheduled
    """
    try:
        if processed_keys is None:
            processed_keys = set()  # Initialize to empty set if None

        # Get orders that have production logs
        orders_with_logs = set()
        for log in production_logs:
            orders_with_logs.add(log['order_id'])

        print(f"Orders with production logs: {orders_with_logs}")

        # Find the latest end time from all rescheduled operations (orders with logs)
        # FIXED: Only consider rescheduled items on the specified machines (if machine_ids is provided)
        latest_reschedule_end_time = None
        with db_session:
            # Get the latest end time from rescheduled items on the specified machines only
            query = select(ri for ri in RescheduledItem if ri.status == 'reschedule')
            if machine_ids:
                query = query.filter(lambda ri: ri.machine.id in machine_ids)

            latest_rescheduled = query.order_by(lambda ri: desc(ri.end_time)).first()
            if latest_rescheduled:
                latest_reschedule_end_time = latest_rescheduled.end_time
                machine_context = f" on machines {machine_ids}" if machine_ids else ""
                print(f"Latest rescheduled operation ends at: {latest_reschedule_end_time}{machine_context}")

        # Get orders that need to be rescheduled (not in orders_with_logs)
        orders_to_reschedule = set()
        already_rescheduled_operations = set()  # This tracks items already in DB, not just current run

        # Get operations that have already been rescheduled to avoid duplicates
        # FIXED: Only consider operations on the specified machines
        with db_session:
            query = select(ri for ri in RescheduledItem if ri.status == 'reschedule')
            if machine_ids:
                query = query.filter(lambda ri: ri.machine.id in machine_ids)

            existing_rescheduled = query
            for item in existing_rescheduled:
                already_rescheduled_operations.add(item.operation.id)

        machine_context = f" on machines {machine_ids}" if machine_ids else ""
        print(f"Operations already rescheduled{machine_context}: {already_rescheduled_operations}")

        # Find orders that need cascade rescheduling
        if schedule_df is not None and not schedule_df.empty:
            for idx, row in schedule_df.iterrows():
                try:
                    # Get operation and order info
                    production_order = row.get('production_order') or row.get('Production Order')
                    operation_name = row.get('operation') or row.get('Operation')
                    machine_id = row.get('machine_id') or row.get('Machine ID')

                    if not all([production_order, operation_name, machine_id]):
                        continue

                    # Machine filter (only reschedule orders on specific machines if provided)
                    if machine_ids is not None and machine_id not in machine_ids:
                        continue

                    # Find the operation
                    operation = select(op for op in Operation
                                       if op.operation_description == operation_name
                                       and op.machine.id == machine_id
                                       and op.order.production_order == production_order).first()

                    if not operation:
                        continue

                    order_id = operation.order.id

                    # Skip if this order already has production logs
                    if order_id in orders_with_logs:
                        continue

                    # Skip if this operation has already been processed in the current run (via processed_keys)
                    if processed_keys is not None and (operation.id, order_id) in processed_keys:
                        print(
                            f"Skipping operation {operation.id} (order {order_id}) - already processed in current run")
                        continue

                    # Skip default machines
                    if is_default_machine(machine_id):
                        continue

                    # Add this order to the list of orders to reschedule
                    if order_ids_allowlist is None or order_id in order_ids_allowlist:
                        orders_to_reschedule.add(order_id)

                except Exception as e:
                    print(f"Error processing row {idx}: {e}")
                    continue
        else:
            # No schedule_df available → discover candidate orders directly from DB by machine
            with db_session:
                # 1) Restrict to orders that are ACTIVE in PartScheduleStatus
                active_pairs = set(
                    select((ps.part_number, ps.production_order) for ps in PartScheduleStatus if ps.status == 'active')[
                    :])

                # 2) Restrict to orders that exist in initial scheduled items on the target machines
                scheduled_order_ids = set()
                scheduled_query = select(ri for ri in RescheduledItem if ri.status == 'scheduled')
                if machine_ids is not None:
                    scheduled_query = scheduled_query.filter(lambda ri: ri.machine.id in machine_ids)
                for ri in scheduled_query:
                    try:
                        if ri.order and (ri.order.part_number, ri.order.production_order) in active_pairs:
                            scheduled_order_ids.add(ri.order.id)
                    except Exception:
                        continue

                if not scheduled_order_ids:
                    machine_context = f" on machines {machine_ids}" if machine_ids else ""
                    print(f"No active scheduled orders found{machine_context} for cascading")

                # 3) From operations on the machines, include only those whose order is in the active+scheduled set
                query = select(op for op in Operation if op.order is not None)
                if machine_ids is not None:
                    query = query.filter(lambda op: op.machine.id in machine_ids)
                for op in query:
                    try:
                        order = op.order
                        if order.id not in scheduled_order_ids:
                            continue
                        # Skip setup-only orders
                        total_qty = getattr(order, 'launched_quantity', 1)
                        if total_qty <= 1:
                            continue
                        # Skip default machines
                        if is_default_machine(op.machine.id):
                            continue
                        # Skip orders that already have production logs
                        if order.id in orders_with_logs:
                            continue
                        # Skip if this operation has already been processed in the current run (via processed_keys)
                        if processed_keys is not None and (op.id, order.id) in processed_keys:
                            print(f"Skipping operation {op.id} (order {order.id}) - already processed in current run")
                            continue

                        if order_ids_allowlist is None or order.id in order_ids_allowlist:
                            orders_to_reschedule.add(order.id)
                    except Exception as e:
                        print(f"Error discovering order for op {getattr(op, 'id', None)}: {e}")
                        continue

        print(f"Orders to reschedule: {orders_to_reschedule}")

        # Get operations for each order that needs rescheduling
        operations_to_reschedule = []
        for order_id in orders_to_reschedule:
            try:
                # Get all operations for this order
                with db_session:
                    order_operations = select(op for op in Operation
                                              if op.order.id == order_id).order_by(lambda op: op.operation_number)[:]

                    for operation in order_operations:
                        # Machine filter - only include operations on specified machines
                        if machine_ids is not None and operation.machine.id not in machine_ids:
                            continue

                        # Check if this operation is already completed based on production logs
                        op_completed_qty = 0
                        op_has_logs = False
                        op_latest_end_time = None
                        with db_session:
                            for log in ProductionLog.select(lambda pl: pl.operation == operation):
                                if getattr(log, 'quantity_completed', None):
                                    op_completed_qty += int(log.quantity_completed)
                                if getattr(log, 'end_time', None) and (
                                        op_latest_end_time is None or log.end_time > op_latest_end_time):
                                    op_latest_end_time = log.end_time
                                op_has_logs = True

                        op_total_qty = getattr(operation.order, 'launched_quantity', 1)
                        op_remaining_qty = max(0, int(op_total_qty) - int(op_completed_qty))

                        # If the operation is fully completed, skip it
                        if op_remaining_qty == 0 and op_has_logs:
                            print(
                                f"Skipping operation {operation.id} ({operation.operation_description}) - fully completed based on logs.")
                            if processed_keys is not None:  # Ensure it's marked as processed
                                processed_keys.add((operation.id, order_id))
                            continue

                        # Skip if this operation has already been rescheduled in the database
                        if operation.id in already_rescheduled_operations:
                            print(
                                f"Skipping operation {operation.id} ({operation.operation_description}) - already rescheduled in DB")
                            continue

                        # Skip if this operation has already been processed in the current run (via processed_keys)
                        if processed_keys is not None and (operation.id, order_id) in processed_keys:
                            print(
                                f"Skipping operation {operation.id} (order {order_id}) - already processed in current run")
                            continue

                        # Skip default machines
                        if is_default_machine(operation.machine.id):
                            continue

                        # If it's a setup op that has already been implicitly covered by a log/previous cascade
                        # and the operation itself doesn't need rescheduling (e.g. 0 remaining qty)
                        # then we explicitly skip it to avoid creating new setup entries
                        current_op_key = (operation.id, order_id)
                        if getattr(operation.order, 'launched_quantity',
                                   1) == 1:  # Assumes launched_quantity is total_qty for setup
                            if processed_keys is not None and current_op_key in processed_keys:
                                print(f"Skipping setup operation {operation.id} (order {order_id}) - already processed")
                                continue
                            pass  # Let cascade logic in dynamic_algorithm handle it

                        # This operation needs to be rescheduled
                        operations_to_reschedule.append({
                            'operation_id': operation.id,
                            'order_id': order_id,
                            'operation_name': operation.operation_description,
                            'production_order': operation.order.production_order,
                            'remaining_qty': op_remaining_qty,  # Pass calculated remaining quantity
                            'has_logs': op_has_logs,  # Pass if this specific operation has logs
                            'latest_log_end_time': op_latest_end_time  # Pass latest log end time
                        })

            except Exception as e:
                print(f"Error processing order {order_id}: {e}")
                continue

        print(f"Found {len(operations_to_reschedule)} operations to reschedule without logs")

        # Reschedule each order (only the first operation, cascade will handle the rest)
        rescheduled_count = 0
        processed_orders = set()  # Track orders processed in this execution

        # Group operations by order
        operations_by_order = {}
        for op_info in operations_to_reschedule:
            order_id = op_info['order_id']
            if order_id not in operations_by_order:
                operations_by_order[order_id] = []
            operations_by_order[order_id].append(op_info)

        for order_id, order_operations in operations_by_order.items():
            try:
                # Skip if this order has already been processed
                if order_id in processed_orders:
                    print(f"Skipping order {order_id} - already processed")
                    continue

                # Check if the *order* itself has been fully processed (all its ops in processed_keys)
                order_fully_processed = True
                for op_info in order_operations:
                    if processed_keys is None or (op_info['operation_id'], op_info['order_id']) not in processed_keys:
                        order_fully_processed = False
                        break
                if order_fully_processed:
                    print(f"Skipping order {order_id} - all operations already processed")
                    processed_orders.add(order_id)
                    continue

                # Get the first operation for this order that hasn't been processed yet
                first_operation_to_reschedule = None
                for op_info in sorted(order_operations, key=lambda x: x['operation_id']):
                    if processed_keys is None or (op_info['operation_id'], op_info['order_id']) not in processed_keys:
                        first_operation_to_reschedule = op_info
                        break

                if not first_operation_to_reschedule:
                    print(f"No unprocessed operations found for order {order_id}")
                    processed_orders.add(order_id)
                    continue

                # Double-check if this operation has been rescheduled in the database
                with db_session:
                    existing = select(ri for ri in RescheduledItem
                                      if ri.operation.id == first_operation_to_reschedule['operation_id']
                                      and ri.status == 'reschedule').first()
                    if existing:
                        print(
                            f"Skipping order {order_id} - first operation {first_operation_to_reschedule['operation_name']} already rescheduled in DB")
                        processed_orders.add(order_id)
                        processed_keys.add(
                            (first_operation_to_reschedule['operation_id'], first_operation_to_reschedule['order_id']))
                        continue

                # Determine starting point for this order
                cascade_start_time = forced_start_time
                if cascade_start_time is None:
                    # Fallback to latest reschedule end time if available, otherwise original start
                    if latest_reschedule_end_time:
                        cascade_start_time = latest_reschedule_end_time
                        print(f"Using latest reschedule end time as cascade start: {cascade_start_time}")
                    elif first_operation_to_reschedule['latest_log_end_time']:
                        cascade_start_time = first_operation_to_reschedule['latest_log_end_time']
                        print(f"Using operation's latest log end time as cascade start: {cascade_start_time}")
                    else:
                        # Fallback to original start time from schedule_df if no rescheduled operations exist
                        original_start = None
                        if schedule_df is not None and not schedule_df.empty:
                            matching_rows = schedule_df[
                                (schedule_df.get('operation') == first_operation_to_reschedule['operation_name']) &
                                (schedule_df.get('production_order') == first_operation_to_reschedule[
                                    'production_order'])
                                ]
                            if not matching_rows.empty:
                                original_start = matching_rows.iloc[0].get('start_time') or matching_rows.iloc[0].get(
                                    'Start Time')
                                if hasattr(original_start, 'to_pydatetime'):
                                    original_start = original_start.to_pydatetime()

                        if not original_start:
                            print(
                                f"Could not find original start time for {first_operation_to_reschedule['operation_name']}")
                            continue

                        cascade_start_time = original_start
                        print(f"Using original start time as fallback: {cascade_start_time}")

                if cascade_start_time is None:
                    print(f"Could not determine start time for order {order_id}. Skipping.")
                    continue

                # Get the order's required quantity
                order_db = Order.get(id=order_id)
                if not order_db:
                    continue

                # Use the remaining_qty determined for this specific operation
                remaining_qty_for_op = first_operation_to_reschedule['remaining_qty']
                has_logs_for_op = first_operation_to_reschedule['has_logs']

                print(
                    f"Rescheduling order {order_id} starting with {first_operation_to_reschedule['operation_name']} with qty {remaining_qty_for_op}")
                print(f"  Starting from: {cascade_start_time}")

                # Reschedule the first unprocessed operation and cascade to all dependent operations
                # Pass processed_keys to cascade function to ensure it also marks operations
                ok = reschedule_operation_with_cascade(
                    operation_id=first_operation_to_reschedule['operation_id'],
                    order_id=order_id,
                    remaining_qty=remaining_qty_for_op,
                    production_log_end_time=cascade_start_time,
                    has_production_logs=has_logs_for_op  # Pass if this specific operation has logs
                )

                if ok:
                    rescheduled_count += 1
                    processed_orders.add(order_id)
                    # Add all operations of this order to processed_keys after successful cascade
                    with db_session:
                        all_ops_for_order = select(op for op in Operation if op.order.id == order_id)[:]
                        for op_item in all_ops_for_order:
                            if processed_keys is not None:  # Add this check
                                processed_keys.add((op_item.id, order_id))  # Add all ops of this order

                    print(f"  Successfully rescheduled order {order_id}")

                    # Update the latest reschedule end time for next orders (machine-specific)
                    with db_session:
                        query = select(ri for ri in RescheduledItem
                                       if ri.status == 'reschedule' and ri.order.id == order_id)
                        if machine_ids:
                            query = query.filter(lambda ri: ri.machine.id in machine_ids)

                        latest_rescheduled_item = query.order_by(lambda ri: desc(ri.end_time)).first()
                        if latest_rescheduled_item:
                            latest_reschedule_end_time = latest_rescheduled_item.end_time
                            print(f"  Updated latest reschedule end time: {latest_reschedule_end_time}")
                else:
                    print(f"  Failed to reschedule order {order_id}")

            except Exception as e:
                print(f"Error rescheduling order {order_id}: {e}")
                continue

        return rescheduled_count

    except Exception as e:
        print(f"Error in reschedule_operations_without_logs: {e}")
        return 0


def store_schedule_in_rescheduled_items(schedule_df: pd.DataFrame) -> int:
    """Store scheduling results in rescheduled_items table with completed_qty = 0"""
    try:
        print(f"Storing schedule with {len(schedule_df)} rows")

        if schedule_df.empty:
            print("Schedule DataFrame is empty, nothing to store")
            return 0

        with db_session:
            items_created = 0

            for idx, row in schedule_df.iterrows():
                try:
                    # Get production order and operation/machine IDs
                    production_order = row.get('production_order') or row.get('Production Order')
                    operation_name = row.get('operation') or row.get('Operation')
                    machine_id = row.get('machine_id') or row.get('Machine ID')

                    if not all([production_order, operation_name, machine_id]):
                        print(
                            f"Missing required fields: production_order={production_order}, operation_name={operation_name}, machine_id={machine_id}")
                        continue

                    # Find operation by name, machine_id and production_order
                    operation = select(op for op in Operation
                                       if op.operation_description == operation_name
                                       and op.machine.id == machine_id
                                       and op.order.production_order == production_order).first()

                    if not operation:
                        print(
                            f"Could not find operation: name={operation_name}, machine_id={machine_id}, production_order={production_order}")
                        continue

                    # Get related entities
                    order = operation.order
                    machine = operation.machine

                    if not all([order, operation, machine]):
                        print(
                            f"Missing entities for row {idx}: order={order}, operation={operation}, machine={machine}")
                        continue

                    # Get quantity and times
                    quantity_str = row.get('quantity') or row.get('Quantity') or '1'
                    start_time = row.get('start_time') or row.get('Start Time')
                    end_time = row.get('end_time') or row.get('End Time')

                    if not all([start_time, end_time]):
                        print(f"Missing time fields for row {idx}: start_time={start_time}, end_time={end_time}")
                        continue

                    # Parse quantity from text like "Setup(6.0/6.0min)" or "Process(6/6pcs)"
                    if isinstance(quantity_str, str):
                        if "Setup" in quantity_str:
                            quantity = 1  # Setup operations are always quantity 1
                            operation_type = "setup"
                        elif "Process" in quantity_str:
                            # Extract number from "Process(6/6pcs)" -> 6
                            match = re.search(r'Process\((\d+)/', quantity_str)
                            quantity = int(match.group(1)) if match else 1
                            operation_type = "cycle"
                        else:
                            try:
                                quantity = int(float(quantity_str))
                                operation_type = "setup" if quantity == 1 else "cycle"
                            except:
                                quantity = 1
                                operation_type = "setup"
                    else:
                        quantity = int(quantity_str) if quantity_str else 1
                        operation_type = "setup" if quantity == 1 else "cycle"

                    # Create INITIAL scheduled item (setup and cycle operations)
                    # These are the original scheduled items that are never overwritten
                    rescheduled_item = RescheduledItem(
                        order=order,
                        operation=operation,
                        machine=machine,
                        start_time=pd.to_datetime(start_time),
                        end_time=pd.to_datetime(end_time),
                        total_qty=int(quantity),
                        completed_qty=0,  # Always 0 for initial schedule
                        remaining_qty=int(quantity),
                        status='scheduled',  # Original scheduled items
                        type=operation_type  # 'setup' or 'cycle'
                    )

                    items_created += 1

                except Exception as e:
                    print(f"Error creating rescheduled item for row {idx}: {e}")
                    continue

            commit()
            print(f"Committed {items_created} rescheduled items to database")
            return items_created

    except Exception as e:
        print(f"Error storing schedule in rescheduled items: {e}")
        raise


@router.post("/run-dynamic-schedule", response_model=DynamicScheduleResponse)
async def run_dynamic_schedule(request: DynamicScheduleRequest = None):
    """
    Run dynamic scheduling process:
    1. Execute scheduling algorithm and store in rescheduled_items with completed_qty = 0
    2. Check production logs for cycle operations (total_qty != 1)
    3. Determine reschedule triggers based on production progress
    4. Re-run scheduling for operations with remaining quantity
    5. Update rescheduled items with 'reschedule' status
    """
    start_execution_time = datetime.now()

    try:
        # Step 1: Get active parts for scheduling
        print("Step 1: Getting active parts for scheduling...")
        component_quantities = get_active_parts_for_scheduling()

        if not component_quantities:
            return DynamicScheduleResponse(
                message="No active parts found for scheduling",
                rescheduled_items_count=0,
                production_logs_processed=0,
                reschedule_triggers=[],
                execution_time=0.0,
                timestamp=datetime.now()
            )

        print(f"Found {len(component_quantities)} active parts")

        # Step 2: Get operations data and run initial scheduling
        print("Step 2: Running initial scheduling algorithm...")
        print("NOTE: Initial scheduling does NOT consider production logs - only uses algorithm")

        with db_session:
            try:
                operations_df = fetch_operations()
            except Exception as e:
                print(f"Error fetching operations: {e}")
                raise HTTPException(status_code=500, detail="Failed to fetch operations data")

            if operations_df.empty:
                raise HTTPException(status_code=500, detail="No operations data available")

            # Run scheduling algorithm (automatically filters out default machines)
            schedule_df, overall_end_time, overall_time, daily_production, component_status, partially_completed = schedule_operations(
                operations_df,
                component_quantities
            )

        if schedule_df.empty:
            raise HTTPException(status_code=500, detail="Scheduling algorithm produced no results")

        print(f"Initial scheduling completed with {len(schedule_df)} operations")

        # Step 3: Clear existing rescheduled items and store new schedule
        print("Step 3: Storing schedule in rescheduled_items...")
        clear_rescheduled_items()
        rescheduled_items_count = store_schedule_in_rescheduled_items(schedule_df)

        # Step 4: Get production logs and process rescheduling
        print("Step 4: Getting production logs and processing rescheduling...")
        print("NOTE: Rescheduling considers production logs and ignores default machines")
        production_logs = get_production_logs_for_rescheduling()
        print(f"Found {len(production_logs)} production logs for rescheduling (excluding default machines)")

        # Step 5: Process reschedule triggers using the new algorithm
        print("Step 5: Processing reschedule triggers...")
        if len(production_logs) == 0:
            print("No production logs found - copying scheduled items to rescheduled items with same timestamps")
            successful_reschedules = copy_scheduled_to_rescheduled_items()
            failed_reschedules = 0
        else:
            successful_reschedules, failed_reschedules, processed_keys = process_reschedule_triggers(production_logs)
        print(f"Rescheduling complete: {successful_reschedules} successful, {failed_reschedules} failed")

        # REMOVED: Global cascade that was causing cross-machine dependencies
        # The machine-specific cascading is already handled properly inside process_reschedule_triggers()
        print("Machine-specific cascading completed within process_reschedule_triggers - no global cascade needed")

        # Calculate execution time
        execution_time = (datetime.now() - start_execution_time).total_seconds()

        # Prepare reschedule trigger information for response
        trigger_info = []
        for log_info in production_logs:
            trigger_info.append({
                "operation_id": log_info['operation_id'],
                "order_id": log_info['order_id'],
                "production_order": log_info['production_order'],
                "part_number": log_info['part_number'],
                "completed_qty": log_info['completed_qty'],
                "remaining_qty": log_info['remaining_qty'],
                "reschedule_reason": f"Production completed {log_info['completed_qty']}/{log_info['total_qty']}, remaining: {log_info['remaining_qty']}"
            })

        # Prepare appropriate message based on whether production logs were found
        if len(production_logs) == 0:
            message = f"Dynamic scheduling completed successfully. {rescheduled_items_count} items scheduled, {successful_reschedules} items copied to rescheduled (no production logs found)."
        else:
            message = f"Dynamic scheduling completed successfully. {rescheduled_items_count} items scheduled, {successful_reschedules} operations rescheduled from production logs."

        return DynamicScheduleResponse(
            message=message,
            rescheduled_items_count=rescheduled_items_count,
            production_logs_processed=len(production_logs),
            reschedule_triggers=trigger_info,
            execution_time=execution_time,
            timestamp=datetime.now()
        )

    except Exception as e:
        print(f"Error in dynamic scheduling: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Dynamic scheduling failed: {str(e)}")


@router.get("/rescheduled-items", response_model=RescheduledItemList)
async def get_rescheduled_items(
        status: Optional[str] = Query(None, description="Filter by status (scheduled, reschedule)"),
        operation_type: Optional[str] = Query(None, description="Filter by type (setup, cycle)"),
        limit: int = Query(100, ge=1, le=1000, description="Number of items to return")
):
    """Get rescheduled items with optional filtering

    Note:
    - 'scheduled' status = original scheduled items (setup and cycle)
    - 'reschedule' status = newly rescheduled items (cycle only, separate rows)
    """
    try:
        with db_session:
            query = select(r for r in RescheduledItem)

            if status:
                query = query.filter(lambda r: r.status == status)

            if operation_type:
                query = query.filter(lambda r: r.type == operation_type)

            # Order by creation time, most recent first
            query = query.order_by(desc(RescheduledItem.created_at))

            items = query.limit(limit)[:]

            response_items = []
            for item in items:
                response_items.append(RescheduledItemResponse(
                    id=item.id,
                    order_id=item.order.id,
                    operation_id=item.operation.id,
                    machine_id=item.machine.id,
                    start_time=item.start_time,
                    end_time=item.end_time,
                    total_qty=item.total_qty,
                    completed_qty=item.completed_qty,
                    remaining_qty=item.remaining_qty,
                    status=item.status,
                    type=item.type,
                    created_at=item.created_at
                ))

            return RescheduledItemList(
                rescheduled_items=response_items,
                total_count=len(response_items)
            )

    except Exception as e:
        print(f"Error getting rescheduled items: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get rescheduled items: {str(e)}")


@router.delete("/clear-rescheduled-items")
async def clear_all_rescheduled_items():
    """Clear all rescheduled items"""
    try:
        clear_rescheduled_items()
        return {"message": "All rescheduled items cleared successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to clear rescheduled items: {str(e)}")


@router.get("/scheduled-items", response_model=RescheduledItemList)
async def get_scheduled_items(
        operation_type: Optional[str] = Query(None, description="Filter by type (setup, cycle)"),
        limit: int = Query(100, ge=1, le=1000, description="Number of items to return")
):
    """Get only the original scheduled items (setup and cycle operations)

    These are the initial scheduled items that are never overwritten.
    Use /rescheduled-items?status=scheduled for the same result.
    """
    try:
        with db_session:
            query = select(r for r in RescheduledItem if r.status == 'scheduled')

            if operation_type:
                query = query.filter(lambda r: r.type == operation_type)

            # Order by creation time, most recent first
            query = query.order_by(desc(RescheduledItem.created_at))

            items = query.limit(limit)[:]

            response_items = []
            for item in items:
                response_items.append(RescheduledItemResponse(
                    id=item.id,
                    order_id=item.order.id,
                    operation_id=item.operation.id,
                    machine_id=item.machine.id,
                    start_time=item.start_time,
                    end_time=item.end_time,
                    total_qty=item.total_qty,
                    completed_qty=item.completed_qty,
                    remaining_qty=item.remaining_qty,
                    status=item.status,
                    type=item.type,
                    created_at=item.created_at
                ))

            return RescheduledItemList(
                rescheduled_items=response_items,
                total_count=len(response_items)
            )

    except Exception as e:
        print(f"Error getting scheduled items: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get scheduled items: {str(e)}")


@router.get("/reschedule-status")
async def get_reschedule_status():
    """Get current reschedule status and statistics"""
    try:
        with db_session:
            total_items = select(r for r in RescheduledItem).count()
            scheduled_items = select(r for r in RescheduledItem if r.status == 'scheduled').count()
            rescheduled_items = select(r for r in RescheduledItem if r.status == 'reschedule').count()
            setup_operations = select(r for r in RescheduledItem if r.type == 'setup').count()
            cycle_operations = select(r for r in RescheduledItem if r.type == 'cycle').count()

            return {
                "total_items": total_items,
                "scheduled_items": scheduled_items,
                "rescheduled_items": rescheduled_items,
                "setup_operations": setup_operations,
                "cycle_operations": cycle_operations,
                "last_updated": datetime.now()
            }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get reschedule status: {str(e)}")


def _format_timedelta(td: timedelta) -> str:
    """Format timedelta to a concise string."""
    total_seconds = int(td.total_seconds())
    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours or days:
        parts.append(f"{hours}h")
    if minutes or hours or days:
        parts.append(f"{minutes}m")
    parts.append(f"{seconds}s")
    return " ".join(parts)


def _minutes_str(delta: timedelta) -> str:
    return str(round(delta.total_seconds() / 60.0, 1))


def _build_combined_response_from_db() -> LegacyCombinedScheduleResponse:
    # Scheduled and rescheduled items
    with db_session:
        scheduled_items = select(r for r in RescheduledItem if r.status == 'scheduled').order_by(
            lambda r: desc(r.start_time))[:]
        rescheduled_items = select(r for r in RescheduledItem if r.status == 'reschedule').order_by(
            lambda r: desc(r.start_time))[:]

        # Compute overall time window across all items
        all_items = scheduled_items + rescheduled_items
        if all_items:
            earliest_start = min((i.start_time for i in all_items if i.start_time is not None), default=None)
            latest_end = max((i.end_time for i in all_items if i.end_time is not None), default=None)
            overall_end_time_value = latest_end
            overall_time_value = _minutes_str(latest_end - earliest_start) if earliest_start and latest_end else "0"
        else:
            overall_end_time_value = datetime.now()
            overall_time_value = "0"

        # Build scheduled_operations (legacy ScheduledOperation)
        scheduled_operations: List[ScheduledOperation] = []
        for item in scheduled_items:
            try:
                order = item.order
                operation = item.operation
                machine = item.machine
                component = getattr(order, 'part_number', '')
                part_desc = getattr(order, 'part_description', None) or component
                description = getattr(operation, 'operation_description', '')
                wc_code = getattr(getattr(machine, 'work_center', None), 'code', None)
                machine_make = getattr(machine, 'make', getattr(machine, 'machine_name', f"Machine-{machine.id}"))
                machine_label = f"{wc_code}-{machine_make}" if wc_code else machine_make
                qty = int(item.total_qty or 0)
                if item.type == 'setup':
                    # Setup operation - calculate actual vs planned setup time
                    try:
                        setup_minutes_total = int(float(operation.setup_time) * 60.0) if hasattr(operation,
                                                                                                 'setup_time') and operation.setup_time else 0
                        actual_minutes = max(0, int((item.end_time - item.start_time).total_seconds() // 60))
                        quantity_str = f"Setup({actual_minutes}/{setup_minutes_total}min)"
                    except Exception:
                        quantity_str = "Setup(0/0min)"
                else:
                    # Cycle operation - for scheduled items, show full quantity as planned
                    if item.status == 'scheduled':
                        # Original scheduled items show full quantity as planned
                        quantity_str = f"Process({qty}/{qty}pcs)"
                    else:
                        # Rescheduled items show actual completed vs remaining
                        completed_qty = int(item.completed_qty or 0)
                        quantity_str = f"Process({completed_qty}/{qty}pcs)"
                scheduled_operations.append(
                    ScheduledOperation(
                        component=str(component),
                        part_description=str(part_desc),
                        description=str(description),
                        machine=str(machine_label),
                        start_time=item.start_time,
                        end_time=item.end_time,
                        quantity=quantity_str,
                        production_order=getattr(order, 'production_order', None)
                    )
                )
            except Exception:
                continue

        # Build reschedule updates (legacy RescheduleUpdate)
        reschedule_updates: List[RescheduleUpdate] = []
        for item in rescheduled_items:
            try:
                order = item.order
                operation = item.operation
                reschedule_updates.append(
                    RescheduleUpdate(
                        operation_id=int(getattr(operation, 'id', item.id)),
                        old_version=0,
                        new_version=1,
                        completed_qty=int(item.completed_qty or 0),
                        remaining_qty=int(item.remaining_qty or 0),
                        start_time=item.start_time.isoformat() if item.start_time else "",
                        end_time=item.end_time.isoformat() if item.end_time else "",
                        machine_id=int(getattr(item.machine, 'id', 0)),
                        raw_material_status='Available',
                        operation_number=int(getattr(operation, 'operation_number', 0) or 0),
                        last_available_operation=int(getattr(operation, 'operation_number', 0) or 0),
                        part_number=str(getattr(order, 'part_number', '')),
                        production_order=str(getattr(order, 'production_order', '')),
                    )
                )
            except Exception:
                continue

    # Production logs aggregate → build minimal responses
    production_logs_resp: List[ProductionLogResponse] = []
    total_completed = 0
    total_rejected = 0
    try:
        with db_session:
            logs = select(l for l in ProductionLog)[:]
            for log in logs:
                try:
                    qty_c = int(getattr(log, 'quantity_completed', 0) or 0)
                    qty_r = int(getattr(log, 'quantity_rejected', 0) or 0)
                    total_completed += qty_c
                    total_rejected += qty_r
                    order = getattr(getattr(log, 'operation', None), 'order', None)
                    machine = getattr(getattr(getattr(log, 'schedule_version', None), 'schedule_item', None), 'machine',
                                      None)
                    # If no machine from schedule_version, try getting from operation directly
                    if not machine:
                        machine = getattr(getattr(log, 'operation', None), 'machine', None)
                    machine_name = None
                    if machine:
                        wc_code = getattr(getattr(machine, 'work_center', None), 'code', None)
                        machine_make = getattr(machine, 'make', getattr(machine, 'machine_name', None))
                        machine_name = f"{wc_code}-{machine_make}" if wc_code and machine_make else (
                                    machine_make or None)
                    production_logs_resp.append(ProductionLogResponse(
                        id=int(getattr(log, 'id', 0)),
                        operator_id=int(getattr(getattr(log, 'operator', None), 'id', 0) or 0),
                        start_time=getattr(log, 'start_time', None),
                        end_time=getattr(log, 'end_time', None),
                        quantity_completed=qty_c,
                        quantity_rejected=qty_r,
                        part_number=str(getattr(order, 'part_number', '')) if order else None,
                        production_order=str(getattr(order, 'production_order', '')) if order else None,
                        operation_description=str(
                            getattr(getattr(log, 'operation', None), 'operation_description', '')),
                        machine_name=machine_name,
                        notes=str(getattr(log, 'notes', '') or ''),
                        version_number=int(getattr(getattr(log, 'schedule_version', None), 'version_number', 0) or 0)
                    ))
                except Exception:
                    continue
    except Exception:
        pass

    # Daily production: aggregate by component and end date from scheduled operations
    daily_production: Dict[str, Dict[str, int]] = {}
    try:
        for op in scheduled_operations:
            try:
                if op.end_time:
                    date_key = op.end_time.date().isoformat()
                    comp_key = op.component
                    if comp_key not in daily_production:
                        daily_production[comp_key] = {}
                    # Parse quantity from quantity string
                    qty = 0
                    if op.quantity.startswith('Process('):
                        try:
                            inner = op.quantity[len('Process('):].split('pcs', 1)[0]
                            qty = int(inner.split('/')[0])
                        except Exception:
                            qty = 0
                    daily_production[comp_key][date_key] = daily_production[comp_key].get(date_key, 0) + qty
            except Exception:
                continue
    except Exception:
        daily_production = {}

    # Work centers and machines in legacy schema
    work_centers_payload: List[LegacyWorkCenterInfo] = []
    try:
        with db_session:
            wcs = select(wc for wc in WorkCenter).prefetch(WorkCenter.machines)[:]
            for wc in wcs:
                machines = []
                for m in wc.machines:
                    machines.append(LegacyMachineInfo(
                        id=str(m.id),
                        name=getattr(m, 'make', getattr(m, 'machine_name', str(m.id))),
                        model=getattr(m, 'model', ''),
                        type=getattr(m, 'type', '')
                    ))
                work_centers_payload.append(LegacyWorkCenterInfo(
                    work_center_code=getattr(wc, 'code', ''),
                    work_center_name=getattr(wc, 'work_center_name', getattr(wc, 'name', '')) or "",
                    machines=machines,
                    is_schedulable=bool(getattr(wc, 'is_schedulable', True))
                ))
    except Exception:
        pass

    return LegacyCombinedScheduleResponse(
        reschedule=reschedule_updates,
        total_updates=len(reschedule_updates),
        production_logs=production_logs_resp,
        scheduled_operations=scheduled_operations,
        overall_end_time=overall_end_time_value,
        overall_time=overall_time_value,
        daily_production=daily_production,
        total_completed=total_completed,
        total_rejected=total_rejected,
        total_logs=len(production_logs_resp),
        work_centers=work_centers_payload
    )


@router.post("/planned-vs-reschedule", response_model=LegacyCombinedScheduleResponse)
async def planned_vs_reschedule():
    """
    Populate database with the latest planned schedule, process reschedules, and
    return a combined view comparing planned vs rescheduled operations, along with
    production logs, totals, and work center information.
    """
    start_execution_time = datetime.now()

    # 1) Run latest plan and reschedule to populate DB
    with db_session:
        operations_df = None
        try:
            component_quantities = get_active_parts_for_scheduling()
            if not component_quantities:
                component_quantities = {}
            operations_df = fetch_operations()
            if operations_df is None or operations_df.empty:
                raise HTTPException(status_code=500, detail="No operations data available")
            schedule_df, overall_end_time_alg, overall_time_alg, daily_production_alg, component_status, partially_completed = schedule_operations(
                operations_df,
                component_quantities
            )
        except HTTPException:
            raise
        except Exception as e:
            print(f"Error preparing scheduling data: {e}")
            raise HTTPException(status_code=500, detail="Failed to compute schedule")

    try:
        clear_rescheduled_items()
        _ = store_schedule_in_rescheduled_items(schedule_df)
        production_logs_raw = get_production_logs_for_rescheduling()

        if len(production_logs_raw) == 0:
            print("No production logs found - copying scheduled items to rescheduled items with same timestamps")
            copy_scheduled_to_rescheduled_items()
        else:
            successful_reschedules, failed_reschedules, processed_keys_from_triggers = process_reschedule_triggers(
                production_logs_raw)

            # REMOVED: Global cascade that was causing cross-machine dependencies
            # The machine-specific cascading is already handled properly inside process_reschedule_triggers()
            print("Machine-specific cascading completed within process_reschedule_triggers - no global cascade needed")

    except HTTPException:
        raise
    except Exception as e:
        print(f"Population/reschedule step failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to populate schedule/reschedules")

    return _build_combined_response_from_db()


@router.get("/planned-vs-reschedule", response_model=LegacyCombinedScheduleResponse)
async def get_planned_vs_reschedule():
    """
    Read-only combined view. Does NOT recompute or mutate any data.
    Returns the current planned (scheduled) and rescheduled operations from DB,
    plus a snapshot of production logs and aggregates.
    """
    return _build_combined_response_from_db()