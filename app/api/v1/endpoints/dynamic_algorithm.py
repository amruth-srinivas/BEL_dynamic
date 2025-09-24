"""
Dynamic Rescheduling Algorithm
Contains the core logic for rescheduling operations based on production progress
"""

import traceback
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from pony.orm import db_session, select, commit, desc
from app.models import Order, Operation, Machine, RescheduledItem, MachineStatus, Status, ProductionLog
from app.algorithm.scheduling import adjust_to_shift_hours, is_working_day, get_next_working_day
from app.api.v1.endpoints.dynamic_rescheduling import is_machine_in_schedulable_work_center


def _rescheduled_item_exists(order: Order, operation: Operation, start_time: datetime, end_time: datetime,
                             status: str, type_: str) -> bool:
    """Check if an identical rescheduled item already exists to avoid duplicates."""
    with db_session:
        # Convert pandas Timestamp to Python datetime if needed
        if hasattr(start_time, 'to_pydatetime'):
            start_time = start_time.to_pydatetime()
        if hasattr(end_time, 'to_pydatetime'):
            end_time = end_time.to_pydatetime()

        existing = select(ri for ri in RescheduledItem
                          if ri.order == order and
                          ri.operation == operation and
                          ri.start_time == start_time and
                          ri.end_time == end_time and
                          ri.status == status and
                          ri.type == type_).first()
        return existing is not None


# Removed: calculate_setup_end_time is now redundant as setup entries are handled by create_shift_split_schedule_entries
# def calculate_setup_end_time(start_time: datetime, operation: Operation) -> datetime:
#     """Compute setup end time respecting shift hours (single end time, no row splitting)."""
#     from app.algorithm.scheduling import get_shift_end, get_next_shift_start
#
#     setup_minutes = float(operation.setup_time) * 60
#     current_time = adjust_to_shift_hours(start_time)
#     remaining = setup_minutes
#
#     while remaining > 0:
#         shift_end = get_shift_end(current_time)
#         minutes_until_shift_end = (shift_end - current_time).total_seconds() / 60
#         if minutes_until_shift_end <= 0:
#             current_time = get_next_shift_start(shift_end)
#             continue
#         allocate = min(remaining, minutes_until_shift_end)
#         current_time += timedelta(minutes=allocate)
#         remaining -= allocate
#         if remaining > 0:
#             current_time = get_next_shift_start(shift_end)
#
#     return current_time


def create_setup_split_entries(start_time: datetime, operation: Operation) -> List[Dict[str, Any]]:
    """Create shift-aware segments for setup time only (exclude cycle time)."""
    from app.algorithm.scheduling import get_shift_end, get_next_shift_start

    setup_minutes_total = float(operation.setup_time) * 60
    current_time = adjust_to_shift_hours(start_time)
    remaining_minutes = setup_minutes_total
    segments: List[Dict[str, Any]] = []

    while remaining_minutes > 0:
        shift_end = get_shift_end(current_time)
        minutes_until_shift_end = (shift_end - current_time).total_seconds() / 60
        if minutes_until_shift_end <= 0:
            current_time = get_next_shift_start(shift_end)
            continue
        minutes_to_allocate = min(remaining_minutes, minutes_until_shift_end)
        segment_end = current_time + timedelta(minutes=minutes_to_allocate)
        segments.append({
            'start_time': current_time,
            'end_time': segment_end,
            'minutes': minutes_to_allocate
        })
        remaining_minutes -= minutes_to_allocate
        current_time = segment_end
        if remaining_minutes > 0:
            current_time = get_next_shift_start(shift_end)

    return segments


def create_shift_split_schedule_entries(start_time: datetime, operation: Operation, quantity: int,
                                        has_production_logs: bool = False, machine_statuses: Dict[int, Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Create multiple schedule entries for operations that span across shifts
    This matches the main scheduling algorithm logic exactly, including machine OFF periods
    """
    from app.algorithm.scheduling import get_shift_end, get_next_shift_start

    setup_time = float(operation.setup_time) * 60  # Convert to minutes
    cycle_time = float(operation.ideal_cycle_time) * 60  # Convert to minutes

    # If production logs are present, setup was already done - exclude setup time
    if has_production_logs:
        total_minutes = cycle_time * quantity
        print(f"  Excluding setup time (production logs present): {setup_time}min")
    else:
        total_minutes = setup_time + (cycle_time * quantity)
        print(f"  Including setup time (no production logs): {setup_time}min")

    current_time = adjust_to_shift_hours(start_time)
    remaining_minutes = total_minutes
    schedule_entries = []
    cumulative_pieces = 0

    # Get machine statuses if not provided
    if machine_statuses is None:
        machine_statuses = get_machine_statuses()

    # Use the same shift splitting logic as main scheduling
    while remaining_minutes > 0:
        shift_end = get_shift_end(current_time)
        minutes_until_shift_end = (shift_end - current_time).total_seconds() / 60

        if minutes_until_shift_end <= 0:
            current_time = get_next_shift_start(shift_end)
            continue

        minutes_to_allocate = min(remaining_minutes, minutes_until_shift_end)
        segment_end = current_time + timedelta(minutes=minutes_to_allocate)

        # Check for machine OFF periods in this shift
        off_periods = find_machine_off_periods(operation.machine.id, current_time, segment_end, machine_statuses)

        if off_periods:
            # Handle each segment separately, skipping OFF periods
            current_segment_start = current_time
            remaining_minutes_in_segment = minutes_to_allocate
            pieces_processed_in_segment = 0

            for off_start, off_end in off_periods:
                # Process until off_start
                if current_segment_start < off_start:
                    segment_minutes = (off_start - current_segment_start).total_seconds() / 60
                    segment_ratio = segment_minutes / total_minutes if total_minutes > 0 else 0
                    segment_pieces = int(quantity * segment_ratio)

                    new_cumulative = min(cumulative_pieces + segment_pieces, quantity)

                    if segment_minutes > 0:
                        schedule_entries.append({
                            'start_time': current_segment_start,
                            'end_time': off_start,
                            'quantity': segment_pieces,
                            'minutes': segment_minutes
                        })
                        cumulative_pieces = new_cumulative
                        pieces_processed_in_segment += segment_pieces
                        remaining_minutes -= segment_minutes

                # Skip the off period
                current_segment_start = off_end

            # Process after the last off period until segment_end
            if current_segment_start < segment_end:
                segment_minutes = (segment_end - current_segment_start).total_seconds() / 60
                segment_ratio = segment_minutes / total_minutes if total_minutes > 0 else 0
                segment_pieces = quantity - pieces_processed_in_segment  # Remaining pieces

                new_cumulative = min(cumulative_pieces + segment_pieces, quantity)

                if segment_minutes > 0:
                    schedule_entries.append({
                        'start_time': current_segment_start,
                        'end_time': segment_end,
                        'quantity': segment_pieces,
                        'minutes': segment_minutes
                    })
                    cumulative_pieces = new_cumulative
                    remaining_minutes -= segment_minutes
        else:
            # No OFF periods, process normally
            # Calculate pieces for this segment
            remaining_pieces = quantity - cumulative_pieces
            segment_ratio = minutes_to_allocate / total_minutes if total_minutes > 0 else 0
            pieces_for_this_segment = quantity * segment_ratio

            # For the last segment, ensure all remaining pieces are allocated
            if remaining_minutes - minutes_to_allocate <= 0:
                segment_pieces = remaining_pieces
                new_cumulative = quantity
            else:
                new_cumulative = min(cumulative_pieces + int(pieces_for_this_segment), quantity)
                segment_pieces = new_cumulative - cumulative_pieces

                # Ensure we don't have 0 pieces if there's work to be done
                if segment_pieces == 0 and remaining_minutes > 0 and cumulative_pieces < quantity:
                    segment_pieces = 1
                    new_cumulative = min(cumulative_pieces + 1, quantity)

            # Debug logging
            print(
                f"    Segment calculation: ratio={segment_ratio:.3f}, pieces_for_segment={pieces_for_this_segment:.1f}, remaining_pieces={remaining_pieces}, cumulative={cumulative_pieces}->{new_cumulative}, segment_pieces={segment_pieces}")

            # Create schedule entry for this segment
            schedule_entries.append({
                'start_time': current_time,
                'end_time': segment_end,
                'quantity': segment_pieces,
                'minutes': minutes_to_allocate
            })

            cumulative_pieces = new_cumulative
            remaining_minutes -= minutes_to_allocate

        current_time = segment_end

        if remaining_minutes > 0:
            current_time = get_next_shift_start(shift_end)

    return schedule_entries


def calculate_shift_aware_duration(start_time: datetime, operation: Operation, quantity: int,
                                   has_production_logs: bool = False, machine_statuses: Dict[int, Dict[str, Any]] = None) -> Tuple[datetime, timedelta]:
    """
    Calculate the end time and duration for an operation, respecting shift hours (6 AM to 10 PM)
    This is a simplified version that returns the final end time
    """
    schedule_entries = create_shift_split_schedule_entries(start_time, operation, quantity, has_production_logs, machine_statuses)

    if not schedule_entries:
        return start_time, timedelta(0)

    # Return the end time of the last segment
    last_entry = schedule_entries[-1]
    end_time = last_entry['end_time']

    # Calculate total duration
    setup_time = float(operation.setup_time) * 60
    cycle_time = float(operation.ideal_cycle_time) * 60
    if has_production_logs:
        total_minutes = cycle_time * quantity
    else:
        total_minutes = setup_time + (cycle_time * quantity)

    total_duration = timedelta(minutes=total_minutes)
    return end_time, total_duration


def get_machine_statuses() -> Dict[int, Dict[str, Any]]:
    """Fetch machine statuses with their status details (excluding default machines)"""
    try:
        with db_session:
            # Get default machine IDs to exclude
            default_machine_ids = [
                m.id for m in Machine.select()
                if m.type == "Default" and m.make == "Default" and m.model == "Default"
            ]
            
            # Fetch machine statuses with their status details (excluding default machines)
            machine_statuses_query = select((m, ms, s, ms.available_from, ms.available_to)
                                            for m in Machine
                                            for ms in m.status
                                            for s in Status
                                            if ms.status == s and m.id not in default_machine_ids)
            machine_statuses = {
                m.id: {
                    'machine_make': m.make,
                    'status_id': s.id,
                    'status_name': s.name,
                    'status_description': s.description,
                    'machine_status_description': ms.description,
                    'available_from': ms.available_from,  # status start
                    'available_to': ms.available_to  # status end (may be None)
                } for m, ms, s, _, _ in machine_statuses_query
            }
            return machine_statuses
    except Exception as e:
        print(f"Error fetching machine statuses: {e}")
        return {}


def find_machine_off_periods(machine_id: int, start_time: datetime, end_time: datetime, 
                            machine_statuses: Dict[int, Dict[str, Any]]) -> List[Tuple[datetime, datetime]]:
    """Find periods when the machine is OFF within the given time range"""
    try:
        # Get default machine IDs to exclude
        with db_session:
            default_machine_ids = [
                m.id for m in Machine.select()
                if m.type == "Default" and m.make == "Default" and m.model == "Default"
            ]
        
        if machine_id in default_machine_ids:
            return [(start_time, end_time)]  # Treat entire period as unavailable

        ms = machine_statuses.get(machine_id)
        off_periods = []

        if not ms or ms['status_name'].upper() != 'OFF':
            return off_periods

        af = ms['available_from']  # Start of OFF period
        at = ms['available_to']  # End of OFF period

        if af and at and af < end_time and at > start_time:
            # Calculate overlap of OFF period with our time range
            overlap_start = max(start_time, af)
            overlap_end = min(end_time, at)

            if overlap_start < overlap_end:
                off_periods.append((overlap_start, overlap_end))

        return off_periods
    except Exception as e:
        print(f"Error finding machine off periods for machine {machine_id}: {e}")
        return []


def check_machine_status(machine_id: int, time: datetime, machine_statuses: Dict[int, Dict[str, Any]] = None) -> Tuple[bool, datetime]:
    """Check if a machine is available at a given time"""
    try:
        # Get default machine IDs to exclude
        with db_session:
            default_machine_ids = [
                m.id for m in Machine.select()
                if m.type == "Default" and m.make == "Default" and m.model == "Default"
            ]
        
        # Skip check for default machines (they shouldn't be scheduled anyway)
        if machine_id in default_machine_ids:
            return False, None

        # First check if it's a working day
        if not is_working_day(time):
            next_working = get_next_working_day(time.replace(hour=6, minute=0, second=0, microsecond=0))
            return False, next_working

        # Use provided machine_statuses or fetch them
        if machine_statuses is None:
            machine_statuses = get_machine_statuses()
        
        ms = machine_statuses.get(machine_id)
        if not ms:
            # No status record → assume machine is available (default)
            return True, time

        status = ms['status_name'].upper()
        af = ms['available_from']  # Start of status period
        at = ms['available_to']  # End of status period (may be None)

        if status == 'OFF':
            # OFF window: unavailable between af and at
            if af and at:
                if af <= time < at:
                    # Time falls within OFF window
                    # Ensure the return time is on a working day
                    next_available = get_next_working_day(at) if not is_working_day(at) else at
                    return False, next_available
                else:
                    # Time is outside OFF window, machine is available
                    return True, time
            elif af and not at:
                # Machine is OFF starting from 'af' indefinitely
                if time >= af:
                    return False, None  # Not available and won't be
                else:
                    return True, time  # Available now until 'af'
            else:
                # Malformed status record
                return True, time

        # Status is ON or any other status
        if status == 'ON':
            if af:
                if time < af:
                    # Machine will be ON from 'af', ensure it's on a working day
                    next_available = get_next_working_day(af) if not is_working_day(af) else af
                    return False, next_available
                else:
                    # Machine is ON now
                    return True, time
            else:
                # Machine is ON with no start time specified
                return True, time

        # Any other status - default to available
        return True, time
    except Exception as e:
        print(f"Error checking machine status for machine {machine_id}: {e}")
        return False, None


def find_last_available_operation(operations: List[Operation], current_time: datetime, machine_statuses: Dict[int, Dict[str, Any]] = None) -> int:
    """Find the last operation that can be performed in sequence"""
    last_available = -1
    current_op_time = current_time

    for idx, op in enumerate(operations):
        machine_id = op.machine.id
        machine_available, available_time = check_machine_status(machine_id, current_op_time, machine_statuses)

        if not machine_available and available_time is None:
            break

        if available_time:
            current_op_time = available_time

        last_available = idx
        setup_time = float(op.setup_time) * 60
        cycle_time = float(op.ideal_cycle_time) * 60
        current_op_time += timedelta(minutes=(setup_time + cycle_time))

    return last_available


def reschedule_operation_with_cascade(operation_id: int, order_id: int, remaining_qty: int,
                                      production_log_end_time: datetime, has_production_logs: bool = True) -> bool:
    """
    Reschedule a single operation and cascade to dependent operations

    Args:
        operation_id: ID of the operation to reschedule
        order_id: ID of the order containing the operation
        remaining_qty: Remaining quantity to be processed
        production_log_end_time: End time from production logs (start time for rescheduling)

    Returns:
        bool: True if rescheduling was successful, False otherwise
    """
    try:
        print(f"Rescheduling operation {operation_id} for order {order_id} with remaining qty {remaining_qty}")

        # Convert pandas Timestamp to Python datetime if needed
        if hasattr(production_log_end_time, 'to_pydatetime'):
            production_log_end_time = production_log_end_time.to_pydatetime()

        # If remaining is 0, treat current op as completed: start next op at production_log_end_time
        current_op_completed = remaining_qty == 0

        with db_session:
            # Get the operation and order
            operation = Operation.get(id=operation_id)
            order = Order.get(id=order_id)

            if not operation or not order:
                print(f"Operation {operation_id} or order {order_id} not found")
                return False

            # IGNORE DEFAULT MACHINES
            if is_default_machine(operation.machine.id):
                print(f"Skipping default machine (machine_id={operation.machine.id}) for operation {operation_id}")
                return False

            # Get machine statuses for handling OFF periods
            machine_statuses = get_machine_statuses()

            # Get all operations for this order, sorted by operation number
            all_operations = list(select(op for op in Operation
                                         if op.order == order).order_by(lambda op: op.operation_number)[:])

            # Find the index of the current operation
            current_op_index = -1
            for i, op in enumerate(all_operations):
                if op.id == operation_id:
                    current_op_index = i
                    break

            if current_op_index == -1:
                print(f"Operation {operation_id} not found in order operations")
                return False

            # Reschedule from the production log end time
            reschedule_start_time = adjust_to_shift_hours(production_log_end_time)
            current_time = reschedule_start_time

            print(f"Starting reschedule from {reschedule_start_time}")

            # Process the current operation and all subsequent operations
            for i in range(current_op_index, len(all_operations)):
                op = all_operations[i]

                # Skip operations that are not in schedulable work centers
                if not hasattr(op, 'machine') or not op.machine:
                    continue
                if not is_machine_in_schedulable_work_center(op.machine.id):
                    print(f"Skipping operation {op.operation_description} as it's in a non-schedulable work center")
                    continue

                # Before creating new rescheduled items for this op,
                # delete any existing 'reschedule' items for this op and order.
                delete_existing_reschedules = select(ri for ri in RescheduledItem
                                                     if ri.operation == op and
                                                     ri.order == order and
                                                     ri.status == 'reschedule')
                if delete_existing_reschedules.count() > 0:
                    print(
                        f"  Deleting {delete_existing_reschedules.count()} existing rescheduled items for operation {op.id}, order {order.id}")
                    delete_existing_reschedules.delete(bulk=True)

                # IGNORE DEFAULT MACHINES during cascade rescheduling
                if is_default_machine(op.machine.id):
                    print(f"Skipping default machine operation {op.operation_description} (machine_id={op.machine.id})")
                    continue

                # Determine quantity and anchoring time for this operation
                # Also detect if this operation already has production logs
                operation_has_logs_local = False
                total_qty = getattr(order, 'launched_quantity', 1)  # Use order's total quantity
                quantity = total_qty  # Default to total quantity for dependent operations

                if i == current_op_index:
                    if remaining_qty == 0:  # If current op has no remaining quantity, it's completed, skip to next op
                        print(
                            f"Operation {i + 1}: Completed in logs, skipping reschedule. Starting next op at {current_time}")
                        continue
                    # For the current operation with remaining work, use remaining quantity
                    quantity = remaining_qty
                    operation_has_logs_local = has_production_logs
                    print(
                        f"Operation {i + 1}: Using remaining quantity {quantity} (has_logs={operation_has_logs_local})")
                else:
                    # For subsequent operations, check if they have their own logs
                    completed_qty_for_op = 0
                    latest_end_for_op = None
                    for log in ProductionLog.select(lambda pl: pl.operation == op):
                        if getattr(log, 'quantity_completed', None):
                            completed_qty_for_op += int(log.quantity_completed)
                        if getattr(log, 'end_time', None) and (
                                latest_end_for_op is None or log.end_time > latest_end_for_op):
                            latest_end_for_op = log.end_time

                    if completed_qty_for_op > 0 or latest_end_for_op is not None:
                        operation_has_logs_local = True
                        # If this operation has logs, anchor its start to its latest log end if it is later than current_time
                        if latest_end_for_op and latest_end_for_op > current_time:
                            current_time = adjust_to_shift_hours(latest_end_for_op)
                            print(
                                f"Operation {i + 1}: Anchoring to its own latest log end {current_time} (was {current_time})")
                        quantity = max(0, total_qty - completed_qty_for_op)
                        print(
                            f"Operation {i + 1}: Detected existing logs (completed={completed_qty_for_op}/{total_qty}), remaining={quantity}")
                    else:
                        # No logs for this dependent operation, use full total_qty
                        quantity = total_qty
                        operation_has_logs_local = False
                        print(f"Operation {i + 1}: No logs found, using full order quantity {quantity}")

                # If nothing remains for this operation, just advance and continue
                if quantity == 0:
                    print(f"Operation {i + 1}: Nothing remaining (qty=0). Skipping reschedule for this op.")
                    continue

                # Track whether we created a separate setup entry
                setup_created = False

                # If no production logs for this operation, store setup separately first
                if not operation_has_logs_local:
                    # Use setup-only shift splitting to avoid including cycle time in setup
                    setup_entries = create_setup_split_entries(current_time, op)

                    if setup_entries:
                        for segment_idx, segment in enumerate(setup_entries):
                            if not _rescheduled_item_exists(order, op, segment['start_time'], segment['end_time'],
                                                            'reschedule', 'setup'):
                                RescheduledItem(
                                    order=order,
                                    operation=op,
                                    machine=op.machine,
                                    start_time=segment['start_time'],
                                    end_time=segment['end_time'],
                                    total_qty=1,
                                    completed_qty=0,
                                    remaining_qty=1,
                                    status='reschedule',
                                    type='setup'
                                )
                                print(
                                    f"  Created SETUP entry segment {segment_idx + 1} for {op.operation_description}: {segment['start_time']} -> {segment['end_time']} (qty=1)")
                                setup_created = True
                            else:
                                print(
                                    f"  Skipped duplicate SETUP entry segment {segment_idx + 1} for {op.operation_description}: {segment['start_time']} -> {segment['end_time']}")

                        if setup_created:
                            current_time = setup_entries[-1]['end_time']
                    else:
                        print(f"  No setup entries created for {op.operation_description}")
                else:
                    print(
                        f"  Setup already covered by production logs or handled in initial setup, skipping setup creation for {op.operation_description}")

                # Create shift-split schedule entries for cycle
                # Exclude setup time if:
                # 1. Operation has production logs (setup already done), OR
                # 2. We just created a separate setup entry (to avoid double counting)
                exclude_setup = operation_has_logs_local or setup_created

                schedule_entries = create_shift_split_schedule_entries(current_time, op, quantity, exclude_setup, machine_statuses)

                # Debug: Show setup and cycle time details
                setup_time = float(op.setup_time) * 60
                cycle_time = float(op.ideal_cycle_time) * 60
                if exclude_setup:
                    total_minutes_for_debug = cycle_time * quantity
                    reason = "has production logs" if operation_has_logs_local else "separate setup created"
                    print(f"  Excluding setup time for {op.operation_description} ({reason}): {setup_time}min")
                else:
                    total_minutes_for_debug = setup_time + (cycle_time * quantity)
                    print(f"  Including setup time for {op.operation_description} (no separate setup): {setup_time}min")
                print(
                    f"Operation {op.operation_description}: {current_time} -> {schedule_entries[-1]['end_time']} (qty: {quantity})")
                print(
                    f"  Setup: {setup_time}min, Cycle: {cycle_time}min/piece, Total: {total_minutes_for_debug}min")  # Used total_minutes_for_debug
                print(f"  Split into {len(schedule_entries)} segments across shifts")

                # Create multiple rescheduled items for cross-shift operations
                for segment_idx, segment in enumerate(schedule_entries):
                    # We are always creating new rescheduled items after deleting old ones, so no need to check _rescheduled_item_exists
                    RescheduledItem(
                        order=order,
                        operation=op,
                        machine=op.machine,
                        start_time=segment['start_time'],
                        end_time=segment['end_time'],
                        total_qty=segment['quantity'],
                        completed_qty=0,  # Always 0 for new rescheduled items (these are future plans)
                        remaining_qty=segment['quantity'],
                        status='reschedule',  # Mark as rescheduled
                        type='cycle'  # Only cycle operations are rescheduled here
                    )
                    print(
                        f"  Segment {segment_idx + 1}: {segment['start_time']} -> {segment['end_time']} (qty: {segment['quantity']})")

                # Move to next operation using the end time of the last segment
                current_time = schedule_entries[-1]['end_time']

            commit()
            print(f"Successfully rescheduled operation {operation_id} and cascaded to dependent operations")
            return True

    except Exception as e:
        print(f"Error rescheduling operation {operation_id}: {e}")
        traceback.print_exc()
        return False


def is_default_machine(machine_id: int) -> bool:
    """Check if a machine is a default machine"""
    with db_session:
        machine = Machine.get(id=machine_id)
        if not machine:
            return False
        return (machine.type == "Default" and
                machine.make == "Default" and
                machine.model == "Default")


def get_production_logs_for_rescheduling() -> List[Dict[str, Any]]:
    """
    Get production logs that indicate operations need rescheduling
    Returns logs for cycle operations (quantity > 1) with remaining work
    NOTE: Setup operations (quantity = 1) are completely ignored
    NOTE: Default machines are completely ignored
    """
    try:
        with db_session:
            # Aggregate logs per operation (and order) by summing completed qty and taking latest end_time
            aggregated: Dict[Tuple[int, int], Dict[str, Any]] = {}

            for log in ProductionLog.select():
                try:
                    # Skip if we don't have required fields
                    if not log.operation or log.quantity_completed is None or not log.end_time:
                        continue

                    # Get related information safely
                    operation = log.operation
                    schedule_version = log.schedule_version
                    schedule_item = schedule_version.schedule_item if schedule_version else None

                    # Strict order mapping:
                    # - Prefer schedule_item.order when present
                    # - But only accept it if it matches operation.order
                    # - Otherwise fall back to operation.order or skip on mismatch
                    order = None
                    if schedule_item and getattr(schedule_item, 'order', None):
                        if schedule_item.order == operation.order:
                            order = schedule_item.order
                        else:
                            # Mismatched mapping between schedule_item.order and operation.order → skip
                            print(
                                f"Skipping log {getattr(log, 'id', 'unknown')} due to order mismatch: schedule_item.order != operation.order")
                            continue
                    else:
                        order = operation.order

                    if not order:
                        continue

                    # If schedule_item has operation, ensure it matches the log.operation too
                    try:
                        schedule_item_operation = getattr(schedule_item, 'operation', None)
                        if schedule_item_operation is not None and schedule_item_operation != operation:
                            print(
                                f"Skipping log {getattr(log, 'id', 'unknown')} due to operation mismatch: schedule_item.operation != log.operation")
                            continue
                    except Exception:
                        pass

                    # IGNORE DEFAULT MACHINES
                    if is_default_machine(log.machine_id):
                        print(f"Skipping default machine (machine_id={log.machine_id}) for operation {operation.id}")
                        continue

                    # Get total quantity - try multiple sources
                    total_qty = 1  # Default
                    if schedule_item and getattr(schedule_item, 'total_quantity', None):
                        total_qty = schedule_item.total_quantity
                    elif getattr(order, 'launched_quantity', None):
                        total_qty = order.launched_quantity

                    # Only include cycle operations (total_qty > 1) - IGNORE SETUP OPERATIONS
                    if total_qty <= 1:
                        print(f"Skipping setup operation (total_qty={total_qty}) for operation {operation.id}")
                        continue

                    key = (operation.id, order.id)
                    if key not in aggregated:
                        aggregated[key] = {
                            'operation_id': operation.id,
                            'order_id': order.id,
                            'production_order': order.production_order if hasattr(order, 'production_order') else None,
                            'part_number': order.part_number if hasattr(order, 'part_number') else None,
                            'machine_id': log.machine_id,
                            'completed_qty': 0,
                            'end_time': log.end_time,
                            'total_qty': total_qty,
                        }

                    agg = aggregated[key]
                    agg['completed_qty'] += int(log.quantity_completed)
                    # track latest end_time
                    if log.end_time and log.end_time > agg['end_time']:
                        agg['end_time'] = log.end_time

                except Exception as e:
                    print(f"Error processing production log {getattr(log, 'id', 'unknown')}: {e}")
                    continue

            # Build final list with remaining quantities
            production_logs: List[Dict[str, Any]] = []
            for key, agg in aggregated.items():
                remaining_qty = max(0, int(agg['total_qty']) - int(agg['completed_qty']))
                # Include even when remaining_qty == 0 so that we can advance to next operation from latest end_time
                payload = {
                    **agg,
                    'remaining_qty': remaining_qty,
                }
                production_logs.append(payload)

                # Debug logging
                print(
                    f"Found production log: Order {agg['order_id']}, Operation {agg['operation_id']}, Part {agg['part_number']}, Completed {agg['completed_qty']}/{agg['total_qty']}, Remaining {remaining_qty}")

            print(f"Total production logs found for rescheduling: {len(production_logs)}")
            return production_logs

    except Exception as e:
        print(f"Error getting production logs: {e}")
        return []


def process_reschedule_triggers(production_logs: List[Dict[str, Any]]) -> Tuple[int, int, set]:  # Modified return type
    """
    Process production logs machine-by-machine, sequencing orders by priority and cascading:
    - For each machine, sort logged orders by priority (lower number = higher priority)
    - Reschedule each logged order in that sequence, anchoring each to max(previous end, its log end)
    - After logged orders, cascade no-log orders on that machine from the latest end

    Returns:
        Tuple[int, int]: (successful_reschedules, failed_reschedules)
    """
    from app.api.v1.endpoints.dynamic_schedule import reschedule_operations_without_logs  # local import to avoid cycles

    successful_reschedules = 0
    failed_reschedules = 0
    default_machine_skips = 0

    print(f"Processing {len(production_logs)} production logs for rescheduling")

    # If no production logs, return early - no rescheduling needed
    if len(production_logs) == 0:
        print("No production logs found - no rescheduling triggers to process")
        return 0, 0, set()

    # Group logs by machine
    logs_by_machine: Dict[int, List[Dict[str, Any]]] = {}
    for pl in production_logs:
        mid = pl.get('machine_id')
        if mid is None:
            continue
        logs_by_machine.setdefault(mid, []).append(pl)

    # Process per machine
    for mid, logs in logs_by_machine.items():
        print(f"\n=== Machine {mid}: processing {len(logs)} logged orders ===")

        # Filter out default machines entirely
        if is_default_machine(mid):
            print(f"  SKIPPING - Machine {mid} is a default machine")
            default_machine_skips += len(logs)
            continue

        # Attach priority to each log/order
        enriched: List[Tuple[int, Dict[str, Any]]] = []  # (priority, log)
        with db_session:
            for log in logs:
                try:
                    order = Order.get(id=log['order_id'])
                    priority = order.project.priority if order and getattr(order, 'project', None) else float('inf')
                except Exception:
                    priority = float('inf')
                enriched.append((priority, log))

        # Sort by ascending priority (lower is higher priority)
        enriched.sort(key=lambda x: x[0])

        # Prepare active scheduled order priorities for this machine once
        from app.api.v1.endpoints.dynamic_schedule import reschedule_operations_without_logs
        import pandas as pd
        order_priority_map: Dict[int, int] = {}
        with db_session:
            # active pairs
            from app.models import PartScheduleStatus
            active_pairs = set(
                select((ps.part_number, ps.production_order) for ps in PartScheduleStatus if ps.status == 'active')[:])

            scheduled_query = select(ri for ri in RescheduledItem if ri.status == 'scheduled' and ri.machine.id == mid)
            for ri in scheduled_query:
                try:
                    order = ri.order
                    if not order or (order.part_number, order.production_order) not in active_pairs:
                        continue
                    prio = order.project.priority if getattr(order, 'project', None) else float('inf')
                    order_priority_map[order.id] = prio
                except Exception:
                    continue

        # Chain reschedules in priority order
        # Initialize machine_anchor to the latest logged end on this machine (if any)
        latest_logged_end: Optional[datetime] = None
        for _prio, _log in enriched:
            try:
                end_t = _log.get('end_time')
                if end_t and (latest_logged_end is None or end_t > latest_logged_end):
                    latest_logged_end = end_t
            except Exception:
                pass
        machine_anchor: Optional[datetime] = latest_logged_end
        cascaded_priorities: set = set()
        processed_keys: set = set()  # (operation_id, order_id) processed in this machine pass

        # Pre-cascade: run all no-log orders with priority higher than the lowest-priority logged order
        try:
            if order_priority_map and enriched:
                lowest_logged_priority = min(p for p, _ in enriched)
                higher_no_log_priorities = sorted(
                    {p for p in order_priority_map.values() if p < lowest_logged_priority})
                if higher_no_log_priorities:
                    from app.api.v1.endpoints.dynamic_schedule import reschedule_operations_without_logs
                    for prio in higher_no_log_priorities:
                        allowlist = {oid for oid, p in order_priority_map.items() if p == prio}
                        if not allowlist:
                            continue
                        print(
                            f"  Pre-cascading higher-priority no-log orders on machine {mid} with priority {prio} from {machine_anchor}")
                        _ = reschedule_operations_without_logs(
                            None,
                            logs,  # pass machine logs to avoid logged/completed orders
                            forced_start_time=machine_anchor,
                            machine_ids={mid},
                            order_ids_allowlist=allowlist
                        )
                        # Refresh anchor after each pre-cascade
                        with db_session:
                            latest_on_machine = select(
                                ri for ri in RescheduledItem if ri.status == 'reschedule' and ri.machine.id == mid) \
                                .order_by(lambda ri: desc(ri.end_time)).first()
                            if latest_on_machine:
                                machine_anchor = latest_on_machine.end_time
                # Ensure per-order anchor starts from refreshed machine_anchor
        except Exception as e:
            print(f"  Pre-cascade error on machine {mid}: {e}")

        for rank, (priority, log_info) in enumerate(enriched, start=1):
            print(f"  -> Order {log_info['order_id']} (priority={priority})")

            # Skip if this (operation, order) was already handled earlier in this pass
            key = (log_info.get('operation_id'), log_info.get('order_id'))
            if key in processed_keys:
                print(f"     Skipping already processed op {key[0]} for order {key[1]}")
                continue

            # Determine anchor: max of current machine anchor and this log's end
            anchor = log_info.get('end_time')
            if machine_anchor and anchor and machine_anchor > anchor:
                anchor = machine_anchor

            # If remaining qty is 0 for this logged operation, find next operation to reschedule
            if int(log_info.get('remaining_qty', 0)) == 0:
                print(
                    f"     Remaining qty is 0 for order {log_info['order_id']}. Finding next operation to reschedule from {anchor}")

                try:
                    # Find the next operation in the same order that needs rescheduling
                    with db_session:
                        order = Order.get(id=log_info['order_id'])
                        if not order:
                            print(f"     Order {log_info['order_id']} not found")
                            continue

                        # Get all operations for this order, sorted by operation number
                        all_operations = list(select(op for op in Operation
                                                     if op.order == order).order_by(lambda op: op.operation_number)[:])

                        # Find the current operation index
                        current_op_index = -1
                        for i, op in enumerate(all_operations):
                            if op.id == log_info['operation_id']:
                                current_op_index = i
                                break

                        if current_op_index == -1:
                            print(f"     Operation {log_info['operation_id']} not found in order operations")
                            continue

                        # Find the next operation that needs rescheduling
                        next_operation = None
                        for i in range(current_op_index + 1, len(all_operations)):
                            op = all_operations[i]

                            # Skip default machines
                            if is_default_machine(op.machine.id):
                                continue

                            # Check if this operation has production logs
                            has_logs = False
                            completed_qty = 0
                            latest_end = None
                            for log in ProductionLog.select(lambda pl: pl.operation == op):
                                if getattr(log, 'quantity_completed', None):
                                    completed_qty += int(log.quantity_completed)
                                if getattr(log, 'end_time', None) and (latest_end is None or log.end_time > latest_end):
                                    latest_end = log.end_time
                                has_logs = True

                            total_qty = getattr(order, 'launched_quantity', None) or 1
                            remaining_qty = max(0, int(total_qty) - int(completed_qty))

                            # If this operation has remaining work, reschedule it
                            if remaining_qty > 0:
                                next_operation = op
                                next_remaining_qty = remaining_qty
                                next_has_logs = has_logs
                                next_anchor = latest_end if latest_end and latest_end > anchor else anchor
                                print(
                                    f"     Found next operation {op.operation_description} with remaining qty {remaining_qty}")
                                break

                        if next_operation:
                            # Reschedule the next operation
                            ok = reschedule_operation_with_cascade(
                                operation_id=next_operation.id,
                                order_id=log_info['order_id'],
                                remaining_qty=next_remaining_qty,
                                production_log_end_time=next_anchor,
                                has_production_logs=next_has_logs
                            )
                            # Mark this next operation as processed to avoid duplicate rescheduling
                            processed_keys.add((next_operation.id, log_info['order_id']))
                            if ok:
                                successful_reschedules += 1
                                # Update machine anchor to latest rescheduled end on this machine
                                with db_session:
                                    latest_on_machine = select(ri for ri in RescheduledItem if
                                                               ri.status == 'reschedule' and ri.machine.id == mid) \
                                        .order_by(lambda ri: desc(ri.end_time)).first()
                                    if latest_on_machine:
                                        machine_anchor = latest_on_machine.end_time
                                # Ensure subsequent cascades use the updated anchor
                                anchor = machine_anchor or anchor
                                print(f"     SUCCESS. Machine anchor -> {machine_anchor}")
                            else:
                                failed_reschedules += 1
                                print(f"     FAILED")
                        else:
                            # No more operations need rescheduling, find the last operation of this order and use its end time
                            with db_session:
                                # Get all operations for this order, sorted by operation number
                                all_operations = list(select(op for op in Operation
                                                             if op.order == order).order_by(
                                    lambda op: op.operation_number)[:])

                                # Find the last operation (highest operation number)
                                last_operation = None
                                for op in all_operations:
                                    if not is_default_machine(op.machine.id):
                                        last_operation = op

                                if last_operation:
                                    # Find the rescheduled end time of the last operation
                                    last_rescheduled = select(ri for ri in RescheduledItem
                                                              if ri.operation == last_operation and
                                                              ri.status == 'reschedule').order_by(
                                        lambda ri: desc(ri.end_time)).first()
                                    if last_rescheduled:
                                        machine_anchor = last_rescheduled.end_time
                                        anchor = machine_anchor
                                        print(
                                            f"     Using last operation {last_operation.operation_description} end time: {machine_anchor}")
                                    else:
                                        # Fallback to latest rescheduled on machine
                                        latest_on_machine = select(ri for ri in RescheduledItem if
                                                                   ri.status == 'reschedule' and ri.machine.id == mid) \
                                            .order_by(lambda ri: desc(ri.end_time)).first()
                                        if latest_on_machine:
                                            machine_anchor = latest_on_machine.end_time
                                        anchor = machine_anchor or anchor
                                else:
                                    # Fallback to latest rescheduled on machine
                                    latest_on_machine = select(ri for ri in RescheduledItem if
                                                               ri.status == 'reschedule' and ri.machine.id == mid) \
                                        .order_by(lambda ri: desc(ri.end_time)).first()
                                    if latest_on_machine:
                                        machine_anchor = latest_on_machine.end_time
                                    anchor = machine_anchor or anchor

                            print(f"     No more operations need rescheduling for order {log_info['order_id']}")

                except Exception as e:
                    failed_reschedules += 1
                    print(f"     ERROR: {e}")

                # Also cascade other scheduled orders on this machine
                try:
                    print(f"     Cascading other scheduled orders from {anchor}")
                    from app.api.v1.endpoints.dynamic_schedule import reschedule_operations_without_logs
                    import pandas as pd

                    # Allowlist: active, scheduled orders on this machine excluding current order
                    allowlist = set(order_priority_map.keys()) - {log_info['order_id']}

                    # Filter out orders that are already processed within this machine's current pass
                    allowlist_filtered = {oid for oid in allowlist if
                                          (log_info['operation_id'], oid) not in processed_keys}

                    if not allowlist_filtered:
                        print("     No other active scheduled orders to cascade on this machine (all processed)")
                        continue

                    _ = reschedule_operations_without_logs(
                        None,  # Changed from pd.DataFrame() to None
                        logs,  # Pass machine logs so orders_with_logs excludes completed/logged orders
                        forced_start_time=anchor,
                        machine_ids={mid},
                        order_ids_allowlist=allowlist_filtered
                    )
                    # Update machine anchor after cascade
                    with db_session:
                        latest_on_machine = select(
                            ri for ri in RescheduledItem if ri.status == 'reschedule' and ri.machine.id == mid) \
                            .order_by(lambda ri: desc(ri.end_time)).first()
                        if latest_on_machine:
                            machine_anchor = latest_on_machine.end_time
                            anchor = machine_anchor  # ensure we use refreshed anchor
                        print(f"     Cascaded other orders. Machine anchor -> {machine_anchor}")
                except Exception as e:
                    print(f"     Error cascading other orders for machine {mid}: {e}")

                continue

            # Before rescheduling this logged order, cascade higher-priority no-log orders once
            if order_priority_map:
                higher_priorities = sorted(
                    {p for p in order_priority_map.values() if p < priority and p not in cascaded_priorities})
                for prio in higher_priorities:
                    allowlist = {oid for oid, p in order_priority_map.items() if p == prio}
                    if not allowlist:
                        continue
                    # Filter out orders that are already processed within this machine's current pass
                    allowlist_filtered = {oid for oid in allowlist if
                                          (log_info['operation_id'], oid) not in processed_keys}

                    if not allowlist_filtered:
                        print(f"  Skipping cascade for priority {prio} (all orders already processed on machine {mid})")
                        cascaded_priorities.add(prio)
                        continue

                    print(f"  Cascading no-log orders on machine {mid} with priority {prio} from {anchor}")
                    try:
                        # Call reschedule_operations_without_logs for this priority band
                        rescheduled_count_for_prio = reschedule_operations_without_logs(
                            None,
                            logs,  # Pass machine logs so orders_with_logs excludes completed/logged orders
                            forced_start_time=anchor,
                            machine_ids={mid},
                            order_ids_allowlist=allowlist_filtered
                        )
                        # If rescheduling was successful, update processed_keys for the newly rescheduled operations
                        if rescheduled_count_for_prio > 0:
                            with db_session:
                                new_rescheduled_items = select(ri for ri in RescheduledItem
                                                               if ri.status == 'reschedule' and ri.machine.id == mid
                                                               and ri.order.id in allowlist_filtered and
                                                               (ri.operation.id,
                                                                ri.order.id) not in processed_keys).distinct()[:]
                                for ri in new_rescheduled_items:
                                    processed_keys.add((ri.operation.id, ri.order.id))

                        # Update machine anchor after cascade and use it for the logged order
                        with db_session:
                            latest_on_machine = select(
                                ri for ri in RescheduledItem if ri.status == 'reschedule' and ri.machine.id == mid) \
                                .order_by(lambda ri: desc(ri.end_time)).first()
                            if latest_on_machine:
                                anchor = latest_on_machine.end_time
                                machine_anchor = anchor
                        cascaded_priorities.add(prio)
                    except Exception as e:
                        print(f"  Priority-based no-log cascade error on machine {mid} (priority {prio}): {e}")

            print(f"     Rescheduling op {log_info['operation_id']} from anchor {anchor}")

            try:
                ok = reschedule_operation_with_cascade(
                    operation_id=log_info['operation_id'],
                    order_id=log_info['order_id'],
                    remaining_qty=log_info['remaining_qty'],
                    production_log_end_time=anchor,
                    has_production_logs=True
                )
                if ok:
                    successful_reschedules += 1
                    # Add the current operation to processed_keys
                    processed_keys.add((log_info['operation_id'], log_info['order_id']))

                    # Update machine anchor to latest rescheduled end on this machine
                    with db_session:
                        latest_on_machine = select(
                            ri for ri in RescheduledItem if ri.status == 'reschedule' and ri.machine.id == mid) \
                            .order_by(lambda ri: desc(ri.end_time)).first()
                        if latest_on_machine:
                            machine_anchor = latest_on_machine.end_time
                    print(f"     SUCCESS. Machine anchor -> {machine_anchor}")
                else:
                    failed_reschedules += 1
                    print(f"     FAILED")
            except Exception as e:
                failed_reschedules += 1
                print(f"     ERROR: {e}")

    print(
        f"Rescheduling complete: {successful_reschedules} successful, {failed_reschedules} failed, {default_machine_skips} default machines skipped")
    return successful_reschedules, failed_reschedules, processed_keys  # Return processed_keys