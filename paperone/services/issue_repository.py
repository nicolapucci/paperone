"""
issue_repository module: Handles insertion, reading, and processing of Issue data.

Main responsibilities:
    - Syncing issue data to/from the local database (upsert_issues, upsert_activity_items)
    - Computing OKR metrics (okr1: bug metrics, okr2: validation metrics, okr4: quality metrics)
    - Tracking issue changes and historical data for analysis
"""

from sqlalchemy import (
    select,
    or_,
    func,
)
from sqlalchemy.orm import Session, aliased

from sqlalchemy.dialects.postgresql import insert

import re
import pytz

from datetime import (
    datetime,
    timezone,
    timedelta
)

from services.logger import logger
from services.postgres_engine import engine
from services.product_repository import ProductRepository

from models.issues import (
    Issue,
    IssueCustomField,
    IssueCustomFieldChange
)   
from models.value import (
    DateValue,
    TimeValue,
    NumberValue,
    StringValue,
    FieldValue,
)

import holidays

import pandas as pd

from services.redis_client import(
    set_okr2_data,
    get_okr2_data,
    set_okr4_data,
    get_okr4_data,
    set_custom_field_id_mapper,
    get_custom_field_id_mapper
)
import uuid
from collections import defaultdict
import bisect

# UTC timezone constant
utc = pytz.UTC

# List of TCoE (Team Center of Excellence) members
TCoE_MEMBERS = ['Sara Tinghi', 'Simona rossi', 'Giuseppe Fragalà', 'Tommaso Capiferri', 'Nicola Montagnani']

# Moving average window: 6 months
ITERVALLO_MEDIA_MOBILE = timedelta(days=(6*30))

# Average duration for a validation activity
avg_val_duration = timedelta(hours=5)

# Percentage of planned hours dedicated to validations (0.0 - 1.0)
validation_time_share = 1

# Total weekly working hours: 40 hours/day * 3 days + 32 hours/day * 2 days
weekly_working_hours = (40*3 + 32*2)

# Italian holidays specific to Pisa province
pisa_holidays = holidays.IT(subdiv='PI')


def working_hours_only_timedelta(end_date: datetime, start_date: datetime):
    """
    Calculate the actual working hours (excluding weekends, holidays, and breaks) between two dates.
    
    Working hours are defined as:
    - Monday to Friday only (excluding Italian holidays in Pisa)
    - 8:00-12:00 and 13:00-17:00 (1 hour lunch break from 12:00-13:00)
    
    Args:
        end_date: End date for the calculation
        start_date: Start date for the calculation
        
    Returns:
        timedelta: Total working hours between the two dates
    """
    end_date = end_date.replace(tzinfo=utc)
    start_date = start_date.replace(tzinfo=utc)

    current_date = start_date
    working_time = timedelta(0)

    while current_date < end_date:
        # Check if current date is a working day (Monday-Friday) and not a holiday
        if current_date.weekday() < 5 and not current_date in pisa_holidays:
            # Define working day boundaries: 8:00-12:00, 13:00-17:00
            working_day_start = datetime(current_date.year, current_date.month, current_date.day, 8, 0, 0).replace(tzinfo=utc)
            working_day_break_start = datetime(current_date.year, current_date.month, current_date.day, 12, 0, 0).replace(tzinfo=utc)
            working_day_break_end = datetime(current_date.year, current_date.month, current_date.day, 13, 0, 0).replace(tzinfo=utc)
            working_day_end = datetime(current_date.year, current_date.month, current_date.day, 17, 0, 0).replace(tzinfo=utc)

            # Calculate overlap between requested time range and working hours
            start = max(current_date, working_day_start)
            end = min(end_date, working_day_end)

            if start < end:
                # Subtract 1 hour for lunch break if the range spans across it
                if start > working_day_break_end or end < working_day_break_start:
                    working_time += (end - start)
                else:
                    working_time += ((end - start) - timedelta(hours=1))

            # Move to the next day
            current_date = datetime(current_date.year, current_date.month, current_date.day, 8, 0, 0).replace(tzinfo=utc) + timedelta(days=1)
        else:
            # Skip to the start of the next working week (Monday 8:00)
            current_date = datetime(current_date.year, current_date.month, current_date.day, 17, 0, 0).replace(tzinfo=utc) + timedelta(days=7 - current_date.weekday())

    return working_time

def extract_fw(string: str):
    """
    Extract firmware version from a string using pattern matching.
    Pattern: X.Y.Z (e.g., 1.2.3)
    
    Args:
        string: String containing firmware version
        
    Returns:
        str: Firmware version in format X.Y.Z
    """
    pattern = r'\d{1,2}\.\d{1,2}\.\d{1,2}'
    match = re.search(pattern, string)
    return match.group(0)

def convert_to_timestamp(date):
    """
    Convert millisecond timestamp to UTC datetime object.
    
    Args:
        date: Millisecond timestamp (integer)
        
    Returns:
        datetime: UTC timezone-aware datetime object
    """
    return datetime.fromtimestamp(date/1000, tz=timezone.utc)

def convert_to_timezone_aware(date):
    """
    Add UTC timezone information to a naive datetime object.
    
    Args:
        date: Naive datetime object
        
    Returns:
        datetime: Timezone-aware datetime object (UTC)
    """
    return date.replace(tzinfo=utc)

def extract_field_name(targetMember: str):
    """
    Extract custom field name from a YouTrack ActivityItem targetMember string.
    Pattern: __CUSTOM_FIELD__<field_name>_<id>
    
    Args:
        targetMember: YouTrack activity item target member string
        
    Returns:
        str: Custom field name, or None if not found
    """
    match = re.search(r'__CUSTOM_FIELD__(\w+(?: \w+)*)_\d+', targetMember)

    if match:
        field_name = match.group(1)
        return field_name
    else:
        return None

def get_value_obj(item, uuid, field_name=None):
    """
    Create a Value object (StringValue, NumberValue, DateValue, or TimeValue) based on item type.
    
    Args:
        item: Data item (dict, string, int, datetime, or timedelta)
        uuid: Field UUID to associate with the value
        field_name: Optional field name for special handling (e.g., 'Estimation', 'Spent time')
        
    Returns:
        Value: One of StringValue, NumberValue, DateValue, or TimeValue
        
    Raises:
        Exception: If item type cannot be classified
    """
    # Special handling for time-based fields that only return numeric values
    misbehaving_targets = ['Estimation', 'Time Left', 'Spent time']
    if field_name in misbehaving_targets:  # Temporary workaround: ActivityItems only return the number, not the possible_keys
        item = {'minutes': item}

    value_possible_keys = ["name", "text", "fullName", "minutes"]
    
    item_value = None

    # Extract value from dictionary using possible keys
    if isinstance(item, dict):
        for possible_key in value_possible_keys:
            if possible_key in item.keys():
                if possible_key == 'minutes':
                    item_value = timedelta(minutes=item.get(possible_key))
                else:
                    item_value = item.get(possible_key)
    else:
        item_value = item

    # Create appropriate Value object based on type
    if isinstance(item_value, str):
        return StringValue(value=item_value, field_id=uuid)
    elif isinstance(item_value, int):
        return NumberValue(value=item_value, field_id=uuid)
    elif isinstance(item_value, datetime):
        return DateValue(value=item_value, field_id=uuid)
    elif isinstance(item_value, timedelta):
        return TimeValue(value=item_value, field_id=uuid)
    else:
        # Unknown type
        logger.warning(f"unable to classify {item} returning None")
        raise


def load_custom_field_mapper():
    """
    Load or create a mapper dictionary: "{field_name}/{issue_id}" -> custom_field_id
    This is cached in Redis for performance.
    
    The mapper allows quick lookup of custom field database IDs using field name and issue ID.
    
    Returns:
        dict: Mapping of "{field_name}/{issue_id_readable}" to IssueCustomField.id
        
    Raises:
        Exception: If database query fails
    """
    # Try to get cached mapper from Redis
    mapper = get_custom_field_id_mapper()
    if mapper:
        return mapper

    # Query database to build mapper
    stmt = (
        select(IssueCustomField.id, IssueCustomField.name, Issue.id_readable)
        .join(Issue, Issue.id_readable == IssueCustomField.issue_id)
    )
    try:
        with Session(engine) as session:
            rows = session.execute(stmt).all()
    except Exception as e:
        logger.error(f"Error loading custom_field_mapper: {e}")
        raise

    # Build mapper dictionary
    mapper = {
        f"{row.name}/{row.id_readable}": row.id
        for row in rows
    }

    # Cache in Redis
    set_custom_field_id_mapper(mapper)

    return mapper


class IssueRepository:
    """
    Repository class for managing issues and computing OKR metrics.
    
    Main methods:
    - get_validation_ids(): Get integration test verification issue IDs
    - upsert_issues(): Sync issues and custom fields to database
    - upsert_activity_items(): Sync historical changes to custom fields
    - okr1(): Generate bug defect rate metrics
    - okr2(): Generate test phase duration and team engagement metrics
    - okr4(): Generate quality and validation turnaround metrics
    """

    @staticmethod
    def get_validation_ids(last_activities_pull=None):
        """
        Get all issue IDs for Integration Test Verification issues.
        Optionally filter by last update time.
        
        Args:
            last_activities_pull: Optional datetime to filter issues updated after this time
            
        Returns:
            list: YouTrack issue IDs matching the criteria
        """
        if last_activities_pull is None:
            stmt = (
                select(Issue.youtrack_id)
                .where(Issue.summary.ilike('%(Integration Test Verification)%'))
            )
        else:
            stmt = (
                select(Issue.youtrack_id)
                .where(Issue.summary.ilike('%(Integration Test Verification)%'))
                .where(Issue.updated >= last_activities_pull)
            )
        with Session(engine) as session:
            r = session.execute(stmt).fetchall()
        
        return [i[0] for i in r]
    
    @staticmethod
    def upsert_issues(issue_data: list):
        """
        Sync issues and their custom fields to the database.
        Updates existing issues or creates new ones.
        
        Process:
        1. Parse issue data and extract custom fields
        2. Upsert issues (INSERT or UPDATE on conflict)
        3. Upsert custom fields (INSERT or UPDATE on conflict)
        4. Create field value and value objects
        
        Args:
            issue_data: List of issue dictionaries from YouTrack API
            
        Raises:
            ValueError: If issue_data is empty or None
            Exception: If database operations fail
        """
        if not issue_data:
            logger.warning(f"Received no Issue data :{issue_data}")
            raise ValueError(f"No issue data provided")

        # Initialize lists for batch insert
        issue_rows = []
        custom_field_rows = []
        field_value_rows = []
        value_rows = []

        for data in issue_data:
            # Extract and convert timestamps
            created = convert_to_timestamp(data.get('created'))
            updated = convert_to_timestamp(data.get('updated'))
            
            # Extract parent issue information
            parent = data.get('parent')
            parent_issues = parent.get('issues', None) if parent else None
            id_readable = data.get('idReadable', None)

            # Extract and normalize tags
            tags = data.get('tags')
            tag_names = []
            for tag in tags:
                tag_name = tag.get('name')
                if tag_name:
                    tag_names.append(tag_name)
                else:
                    logger.debug(f"received tag with no attribute name: {tag}")

            # Handle parent issue ID (keep only first if multiple parents)
            parent_issue_id = None
            if parent_issues:
                if isinstance(parent_issues, list) and parent_issues:
                    parent_issue_id = parent_issues[0].get('idReadable', None)
                elif isinstance(parent_issues, dict):
                    parent_issue_id = parent_issues.get('idReadable', None)
                
            # Add issue row
            issue_rows.append({
                "youtrack_id": data.get('id', None),
                "id_readable": id_readable,
                "summary": data.get('summary'),
                "parent_id": parent_issue_id,
                "created": created,
                "updated": updated,
                "tags": tag_names
            })
            
            # Process custom fields for this issue
            if id_readable:
                for field in data.get('customFields', []):
                    name = field.get('name')
                    value = field.get('value')
                    new_uuid = uuid.uuid4()

                    # Create field value entry
                    field_value_rows.append({"id": new_uuid})
                    
                    # Create custom field entry
                    custom_field_rows.append({
                        "name": name,
                        "value_id": new_uuid,
                        "issue_id": id_readable
                    })

                    # Process field value(s)
                    if value is not None:       
                        if isinstance(value, list):
                            for item in value:
                                try:
                                    value_item = get_value_obj(item, new_uuid, None)
                                    value_rows.append(value_item)
                                except Exception as e:
                                    logger.warning(f"unable to create Value item: {e} -- {item} / {name} / {id_readable}")    
                        else:
                            try:
                                value_item = get_value_obj(value, new_uuid, None)
                                value_rows.append(value_item)
                            except Exception as e:
                                logger.warning(f"unable to create Value item: {value} / {name} / {id_readable}")

        # Build upsert statements
        upsert_issues_stmt = (
            insert(Issue)
            .values(issue_rows)
            .on_conflict_do_update(
                index_elements=["youtrack_id"],
                set_={
                    "summary": insert(Issue).excluded.summary,
                    "updated": insert(Issue).excluded.updated,
                    "parent_id": insert(Issue).excluded.parent_id,
                    "tags": insert(Issue).excluded.tags
                }
            ).returning('*') 
        )

        upsert_custom_fields_stmt = (
            insert(IssueCustomField)
            .values(custom_field_rows)
            .on_conflict_do_update(
                index_elements=["name", "issue_id"],
                set_={
                    "value_id": insert(IssueCustomField).excluded.value_id
                }
            ).returning('*') 
        )

        # Execute upsert in transaction
        with Session(engine) as session:
            try:
                session.execute(insert(FieldValue).values(field_value_rows))
                session.add_all(value_rows)    
                issue_inserted = session.execute(upsert_issues_stmt).fetchall()
                custom_field_inserted = session.execute(upsert_custom_fields_stmt).fetchall()

                logger.info(f"Added/Updated {len(issue_inserted)} Issues and {len(custom_field_inserted)} Custom Fields")
                session.commit() 

            except Exception as e:
                logger.error(f"Error while upserting issues with custom fields: {e}")
                session.rollback()
                raise

    @staticmethod
    def upsert_activity_items(activity_item_data: list):
        """
        Sync historical changes to custom fields to the database.
        Records "added" and "removed" values for each field change with timestamp.
        
        This tracks the evolution of custom field values over time for analysis.
        
        Process:
        1. Load custom field ID mapper
        2. For each activity item, extract field name and changed values
        3. Create field value objects for removed/added values
        4. Create activity item records linking old and new values
        
        Args:
            activity_item_data: List of activity item dictionaries from YouTrack API
            
        Raises:
            Exception: If database operations fail
        """
        activity_item_rows = []
        field_value_rows = []
        value_rows = []

        # Load custom field mapper (cached from Redis if available)
        custom_field_id_mapper = load_custom_field_mapper()

        for data in activity_item_data:
            targetMember = data.get('targetMember')

            # Skip if no target member (field name cannot be determined)
            if targetMember is None:
                issue = data.get('target')
                issue_id = issue.get('idReadable', None) if issue else 'UNKNOWN'
                logger.error(f"{issue_id} has no TargetMember, skipping this activityItem...")
                continue

            # Extract issue and field information
            issue = data.get('target')
            issue_id_readable = issue.get('idReadable', None) if issue else None
            rm = data.get('removed')  # Removed value(s)
            added = data.get('added')  # Added value(s)
            field_name = extract_field_name(targetMember)
            
            # Look up custom field ID
            customField_id = custom_field_id_mapper.get(f"{field_name}/{issue_id_readable}", None)
            
            # Convert timestamp from milliseconds to datetime
            timestamp = data.get('timestamp')
            timestamp = datetime.fromtimestamp(timestamp / 1000) if timestamp else None
            
            added_uuid = None
            rm_uuid = None
            
            if customField_id is None:
                logger.error(f"unable to find custom field for: {issue_id_readable}-{field_name}, skipping this activityItem...")
                continue

            # Log if no changes detected
            if not rm and not added:
                logger.debug(f"{issue_id_readable} \t {field_name} \t {timestamp} - no changes (both added and removed are None)")

            # Process removed value(s)
            if rm:
                rm_uuid = uuid.uuid4()
                field_value_rows.append({"id": rm_uuid})

                if isinstance(rm, list):
                    for item in rm:
                        try:
                            value_obj = get_value_obj(item, rm_uuid, field_name)
                            value_rows.append(value_obj)
                        except Exception as e:
                            logger.warning(f"unable to create removed Value item: {item} / {field_name} / {issue_id_readable} \n{e}")
                else:
                    try:
                        value_obj = get_value_obj(rm, rm_uuid, field_name)
                        value_rows.append(value_obj)
                    except Exception as e:
                        logger.warning(f"unable to create removed Value item: {rm} / {field_name} / {issue_id_readable} \n{e}")

            # Process added value(s)
            if added:
                added_uuid = uuid.uuid4()
                field_value_rows.append({"id": added_uuid})

                if isinstance(added, list):
                    for item in added:
                        try:
                            value_obj = get_value_obj(item, added_uuid, field_name)
                            value_rows.append(value_obj)
                        except Exception as e:
                            logger.warning(f"unable to create added Value item: {item} / {field_name} / {issue_id_readable} \n{e}")
                else:
                    try:
                        value_obj = get_value_obj(added, added_uuid, field_name)
                        value_rows.append(value_obj)
                    except Exception as e:
                        logger.warning(f"unable to create added Value item: {added} / {field_name} / {issue_id_readable} \n{e}")
                
            # Add activity item record linking old and new values
            activity_item_rows.append({
                'field_id': customField_id,
                'old_value_id': rm_uuid,
                'new_value_id': added_uuid,
                'timestamp': timestamp
            })

        # Build insert statements
        insert_field_values_stmt = insert(FieldValue).values(field_value_rows)
        insert_cf_change_stmt = (
            insert(IssueCustomFieldChange)
            .values(activity_item_rows)
            .on_conflict_do_nothing(index_elements=["field_id", "timestamp"])
            .returning(IssueCustomFieldChange.id)
        )

        # Execute in transaction
        with Session(engine) as session:
            try:
                session.execute(insert_field_values_stmt)
                icfc_id = session.execute(insert_cf_change_stmt).fetchall()
                session.add_all(value_rows)

                logger.debug(f'Added {len(icfc_id)} ActivityItems')
                session.commit()

            except Exception as e:
                logger.error(f"Error while upserting activity items: {e}")
                session.rollback()
                raise

    @staticmethod
    def okr1():
        """
        OKR Metric 1: Bug Defect Rate Analysis
        
        Computes bug metrics grouped by origin (customer vs internal) and product.
        Returns the ratio of customer-reported bugs to total bugs for each month,
        as well as breakdowns by origin and product.
        
        Process:
        1. Query all issues with Type='Bug', grouped by creation month
        2. Extract Origin and Product fields for each bug
        3. Calculate defect rate: (customer bugs) / (total bugs)
        4. Check if firmware was released in each month (for context)
        
        Returns:
            list: List of dicts with structure:
                {
                    "date": datetime,
                    "Defect Rate": float (0-1),
                    "FW Released": 0 if released, None if not,
                    "{origin}-{product}-ratio": float (0-1) for each origin-product combo
                }
        """
        # Set up aliases for complex query
        i = aliased(Issue)
        icf = aliased(IssueCustomField)
        sv = aliased(StringValue)

        # CTE 1: Get all bugs grouped by creation month
        bugs_cte = (
            select(
                i.id_readable,
                i.id,
                func.date_trunc('month', i.created).label('date')
            )
            .join(icf, icf.issue_id == i.id_readable)
            .join(sv, icf.value_id == sv.field_id)
            .where(
                icf.name == 'Type',
                sv.value == 'Bug'
            )
        ).cte('bugs_cte')

        # CTE 2: Add bug origin information
        bugs_by_origine_cte = (
            select(
                bugs_cte.c.id_readable,
                bugs_cte.c.id,
                bugs_cte.c.date,
                sv.value.label('origin')
            )
            .join(icf, bugs_cte.c.id_readable == icf.issue_id)
            .join(sv, icf.value_id == sv.field_id, isouter=True)
            .where(icf.name == 'Origine')
        )

        # CTE 3: Add product information
        bugs_by_origine_and_product_cte = (
            select(
                bugs_by_origine_cte.c.id_readable,
                bugs_by_origine_cte.c.id,
                bugs_by_origine_cte.c.date,
                bugs_by_origine_cte.c.origin,
                sv.value.label('product')
            )
            .join(icf, icf.issue_id == bugs_by_origine_cte.c.id_readable)
            .join(sv, icf.value_id == sv.field_id, isouter=True)
            .where(icf.name == 'Product')           
        ).cte('bugs_by_origine_and_product_cte')

        # Final query: Group by date, origin, and product with counts
        bugs_by_origine_and_product_stmt = (
            select(
                bugs_by_origine_and_product_cte.c.date,
                bugs_by_origine_and_product_cte.c.origin,
                bugs_by_origine_and_product_cte.c.product,
                func.count().label('count')
            )
            .group_by(
                bugs_by_origine_and_product_cte.c.date,
                bugs_by_origine_and_product_cte.c.product,
                bugs_by_origine_and_product_cte.c.origin
            )
        )

        # Execute query
        try:
            with Session(engine) as session:
                bugs_by_origine_and_product = session.execute(bugs_by_origine_and_product_stmt).fetchall()
        except Exception as e:
            logger.error(f"Error performing okr1 query: {e}")
            raise

        # Build hierarchical dictionary: date -> origin -> product -> count
        bug_reports_by_date = {}
        for date, origin, product, count in bugs_by_origine_and_product:
            if date not in bug_reports_by_date:
                bug_reports_by_date[date] = {'tot': 0}

            # Accumulate total bugs
            bug_reports_by_date[date]['tot'] += count

            # Create origin entry if needed
            if origin not in bug_reports_by_date[date]:
                bug_reports_by_date[date][origin] = {'tot': 0}
            
            # Accumulate bugs by origin
            bug_reports_by_date[date][origin]['tot'] += count

            # Store product count for this origin
            bug_reports_by_date[date][origin][product] = count

        # Format results for Grafana
        grafana_formatted_result = []
        
        # Get firmware release dates for context
        try:
            changelog_releases = ProductRepository.changelog_releases()
        except Exception as e:
            logger.error(f"Unable to retrieve changelog releases, will not report correctly if a firmware was released.")
            changelog_releases = {}

        # Build final output
        for date, bugs_by_origin_and_product in bug_reports_by_date.items():
            if isinstance(bugs_by_origin_and_product, dict):
                # Extract customer bugs (origin='Cliente')
                total_bugs = bugs_by_origin_and_product['tot']
                customer_bugs = bugs_by_origin_and_product.get('Cliente', {}).get('tot', 0)
                
                grafana_item = {
                    "date": date,
                    "Defect Rate": customer_bugs / total_bugs if total_bugs > 0 else None,
                    "FW Released": None if (date.year, date.month) in [(d.year, d.month) for d in changelog_releases.values()] else 1
                }

                # Add origin-product ratios
                for origin, products in bugs_by_origin_and_product.items():
                    if isinstance(products, dict):
                        for product, count in products.items():
                            if product != 'tot':
                                product_ratio = count / total_bugs
                                grafana_item[f"{origin}-{product}-ratio"] = product_ratio

                grafana_formatted_result.append(grafana_item)
        
        return grafana_formatted_result

    @staticmethod
    def okr2():
        """
        OKR Metric 2: Validation & Testing Phase Duration Analysis
        
        Computes test phase metrics including duration, team effort breakdown, and trends.
        Tracks the time spent on validations, manual tests, and automated tests relative
        to total test phase duration. Includes a 6-month moving average for trend analysis.
        
        Process:
        1. Query RC0 release dates and production release dates
        2. Extract time spent on: validations during FW, manual tests, automated tests
        3. Calculate test phase duration (from RC0 to production)
        4. Compute percentage of time spent on each test type
        5. Calculate 6-month moving average of test phase duration
        6. Cache results in Redis
        
        Returns:
            list: List of dicts with structure:
                {
                    "fw": str (firmware version),
                    "start": int (RC0 timestamp),
                    "test_phase_duration": int (seconds),
                    "validations_time_share": float (%),
                    "manual_time_share": float (%),
                    "automated_time_share": float (%),
                    "other": float (% - remaining time),
                    "media_a_6_mesi": float (moving average duration in seconds)
                }
        """
        i = aliased(Issue)
        icf = aliased(IssueCustomField)
        tv = aliased(TimeValue)

        rc0_releases = ProductRepository.rc0_releases()

        # Get production release dates
        try:
            changelog_releases = ProductRepository.changelog_releases()
        except Exception as e:
            changelog_releases = {}
            logger.error(f"Unable to retrieve changelogs from wiki, test phase duration will be set to 0...")

        # Query: Time spent on validations during firmware testing
        during_fw_stmt = (
            select(
                i.summary,
                tv.value.label('time_spent')
            )
            .select_from(i)
            .join(icf, i.id_readable == icf.issue_id)
            .join(tv, icf.value_id == tv.field_id)
            .where(
                i.summary.ilike('%validation%during%fw%'),
                icf.name.ilike('%Spent time%')
            )
        )

        # Query: Time spent on manual tests
        manual_tests_stmt = (
            select(
                i.summary,
                tv.value.label('time_spent')
            )
            .select_from(i)
            .join(icf, i.id_readable == icf.issue_id)
            .join(tv, icf.value_id == tv.field_id)
            .where(
                i.summary.ilike('test kalliope% - manual testing'),
                icf.name.ilike('%Spent time%')
            )
        )

        # Query: Time spent on automated tests
        automated_tests_stmt = (
            select(
                i.summary,
                tv.value.label('time_spent')
            )
            .select_from(i)
            .join(icf, i.id_readable == icf.issue_id)
            .join(tv, icf.value_id == tv.field_id)
            .where(
                i.summary.ilike('test kalliope% - automated testing'),
                icf.name.ilike('%Spent time%')
            )
        )

        # Execute queries
        try:
            with Session(engine) as session:
                validation_during_fw = session.execute(during_fw_stmt).fetchall()
                automated_tests = session.execute(automated_tests_stmt).fetchall()
                manual_tests = session.execute(manual_tests_stmt).fetchall()
        except Exception as e:
            logger.error(f"Error executing okr2 queries: {e}")
            raise

        # Extract firmware versions and time spent for each test type
        validation_time_spent = {extract_fw(i[0]): i[1] for i in validation_during_fw}
        manual_tests_time_spent = {extract_fw(i[0]): i[1] for i in manual_tests}
        automated_tests_time_spent = {extract_fw(i[0]): i[1] for i in automated_tests}

        # Build per-firmware metrics
        okr2_data = []
        for fw, rc0_release in rc0_releases.items():
            start = rc0_release
            end = changelog_releases.get(fw, None)
            
            # Get time spent on each test type (default to 0 if not found)
            validations = validation_time_spent.get(fw, timedelta(0))
            manual = manual_tests_time_spent.get(fw, timedelta(0))
            automated = automated_tests_time_spent.get(fw, timedelta(0))

            # Calculate test phase duration in absolute and working hours
            test_phase_duration = end - start if end is not None else timedelta(0)
            test_phase_working_hours_duration = working_hours_only_timedelta(end, start) if end is not None else timedelta(0)
            
            # Adjust for team size (weekly_working_hours / 40 hours standard)
            test_phase_team_working_hours_duration = test_phase_working_hours_duration * (weekly_working_hours / 40)

            # Build firmware phase info
            phase_info = {
                "fw": fw,
                "start": int(start.timestamp()),
                "test_phase_duration": int(test_phase_duration.total_seconds()),
            }

            # Calculate time share percentages if test phase has duration
            if test_phase_team_working_hours_duration > timedelta(0):
                phase_info["validations_time_share"] = (validations / test_phase_team_working_hours_duration) * 100
                phase_info["manual_time_share"] = (manual / test_phase_team_working_hours_duration) * 100
                phase_info["automated_time_share"] = (automated / test_phase_team_working_hours_duration) * 100
                phase_info["other"] = (1 - ((validations + manual + automated) / test_phase_team_working_hours_duration)) * 100
            
            okr2_data.append(phase_info)

        # Convert to DataFrame for moving average calculation
        df = pd.DataFrame(okr2_data)
        df["start"] = pd.to_datetime(df["start"], unit='s')
        df = df.sort_values("start")

        # Calculate 6-month moving average of test phase duration
        window_months = 6
        for index, row in df.iterrows():
            current_date = row["start"]
            start_window = current_date - pd.DateOffset(months=window_months)
            
            # Get all previous records within the window
            subset = df[
                (df["start"] < current_date) &
                (df["start"] >= start_window)
            ]
            
            # Calculate average test phase duration
            moving_avg = subset["test_phase_duration"].mean()
            df.loc[index, f"media_a_{window_months}_mesi"] = moving_avg

        # Convert back to dict and cache
        okr2_data = df.to_dict(orient="records")
        set_okr2_data(okr2_data)

        # Retrieve from cache and return
        okr2_data = get_okr2_data()
        return okr2_data


    @staticmethod
    def okr4():
        """
        OKR Metric 4: Quality & Validation Turnaround Analysis
        
        Computes metrics for validation issues (Integration Test Verification) including:
        - Time from creation to assignment to TCoE
        - Time from assignment to completion (Done/Blocked status)
        - Distribution across stages and products
        - Blocking rate and stage breakdowns
        
        Process:
        1. Query all Integration Test Verification issues
        2. Track state transitions: creation -> first assigned -> completion
        3. Calculate wait times for TCoE assignment and turnaround times
        4. Extract firmware version from parent issue
        5. Group metrics by stage and compute distributions
        6. Cache results in Redis
        
        Returns:
            list: List of dicts containing validation metrics including:
                - avg_time_spent: average time spent on validation
                - time_spent_share: percentage of validation lifespan spent on work
                - blocked_share: percentage of time blocked
                - waiting_share: percentage of time waiting
                - queue: average queue position
                - pre/during/slip/blocked/overassigned: stage distribution percentages
        """

        # Set up aliases for complex query
        i = aliased(Issue)
        icf = aliased(IssueCustomField)
        icfc = aliased(IssueCustomFieldChange)
        rm_sv = aliased(StringValue)
        sv = aliased(StringValue)
        tv = aliased(TimeValue)

        rc0_releases = ProductRepository.rc0_releases()

        # Query: Time spent on validation buckets (pre/during FW)
        bucket_stmt = (
            select(
                i.summary,
                tv.value.label('time_spent')
            )
            .select_from(i)
            .join(icf, i.id_readable == icf.issue_id)
            .join(tv, icf.value_id == tv.field_id)
            .where(
                or_(
                    i.summary.ilike('%validation%pre%fw%'),
                    i.summary.ilike('%validation%during%fw%')
                ),
                icf.name.ilike('%Spent time%')
            )
        )

        # CTE 1: Get all Integration Test Verification validations
        validations_cte = (
            select(
                i.id_readable,
                i.created
            )
            .select_from(i)
            .where(i.summary.ilike('(Integration Test Verification)%'))
        ).cte('validations_cte')

        # CTE 2: Get parent issue fix version (firmware being validated)
        parent_fix_version_cte = (
            select(
                i.id_readable,
                sv.value
            )
            .join(icf, i.parent_id == icf.issue_id)
            .join(sv, icf.value_id == sv.field_id)
            .where(icf.name == 'Fix versions')
        ).cte('parent_fix_version_cte')

        # CTE 3: Get completion timestamp (last set as Done or Blocked)
        completions_cte = (
            select(
                validations_cte.c.id_readable,
                func.max(icfc.timestamp).label('last_set_as_done')
            )
            .join(icf, validations_cte.c.id_readable == icf.issue_id)
            .join(icfc, icf.id == icfc.field_id)
            .join(sv, icfc.new_value_id == sv.field_id)
            .where(
                icf.name == 'Stage',
                or_(
                    sv.value == 'Done',
                    sv.value == 'Blocked'
                )
            )
            .group_by(validations_cte.c.id_readable)
        ).cte('completions_cte')

        # CTE 4: Get first TCoE assignment timestamp
        first_assignements_cte = (
            select(
                validations_cte.c.id_readable,
                func.min(icfc.timestamp).label('first_assigned')
            )
            .join(icf, validations_cte.c.id_readable == icf.issue_id)
            .join(icfc, icf.id == icfc.field_id)
            .join(sv, icfc.new_value_id == sv.field_id)
            .where(
                icf.name == 'Assignee',
                sv.value.in_(TCoE_MEMBERS)
            )
            .group_by(validations_cte.c.id_readable)
        ).cte('first_assignements_cte')

        # Query: Join all information about validations
        validations_stmt = (
            select(
                validations_cte.c.id_readable,
                validations_cte.c.created,
                completions_cte.c.last_set_as_done,
                first_assignements_cte.c.first_assigned,
                parent_fix_version_cte.c.value.label('fix_version'),
                sv.value.label('assignee')
            )
            .select_from(validations_cte)
            .join(first_assignements_cte, validations_cte.c.id_readable == first_assignements_cte.c.id_readable, isouter=True)
            .join(completions_cte, validations_cte.c.id_readable == completions_cte.c.id_readable, isouter=True)
            .join(parent_fix_version_cte, validations_cte.c.id_readable == parent_fix_version_cte.c.id_readable)
            .join(icf, validations_cte.c.id_readable == icf.issue_id)
            .join(sv, icf.value_id == sv.field_id)
            .where(icf.name == 'Assignee')
        )

        # CTE 5: Get all stage changes for validations
        validations_stage_changes_cte = (
            select(
                validations_cte.c.id_readable,
                sv.value.label('added'),
                rm_sv.value.label('removed'),
                icfc.timestamp
            )
            .join(icf, validations_cte.c.id_readable == icf.issue_id)
            .join(icfc, icf.id == icfc.field_id)
            .join(sv, icfc.new_value_id == sv.field_id)
            .join(rm_sv, icfc.old_value_id == rm_sv.field_id)
            .where(icf.name == 'Stage')
        ).cte('validations_stage_changes_cte')

        # Query: Get all stage changes with firmware context
        changes_stmt = (
            select(
                validations_stage_changes_cte.c.id_readable,
                validations_stage_changes_cte.c.added,
                validations_stage_changes_cte.c.removed,
                validations_stage_changes_cte.c.timestamp,
                parent_fix_version_cte.c.value.label('fix_version')
            )
            .join(parent_fix_version_cte, validations_stage_changes_cte.c.id_readable == parent_fix_version_cte.c.id_readable)
        )

        # Execute all queries
        try:
            with Session(engine) as session:
                buckets = session.execute(bucket_stmt).fetchall()
                validations = session.execute(validations_stmt).fetchall()
                changes = session.execute(changes_stmt).fetchall()
        except Exception as e:
            logger.error(f"Error executing okr4 queries: {e}")
            raise

        # Build validation changes dictionary: fw -> validation_id -> list of (added, removed, timestamp)
        validation_changes = defaultdict(lambda: defaultdict(list))
        for id_readable, added, removed, timestamp, fix_version in changes:
            validation_changes[fix_version][id_readable].append((added, removed, timestamp))
        
        def sum_blocked_time(changes_list, rc0_release, target='blocked'):
            """
            Calculate total time a validation spent in blocked state.
            Also track if validation slipped due to being blocked across RC0 release.
            
            Args:
                changes_list: List of (added, removed, timestamp) tuples for stage changes
                rc0_release: RC0 release datetime for this firmware
                target: Stage name to track (default: 'blocked')
                
            Returns:
                tuple: (total_blocked_time, slipped_due_to_block_stage)
            """
            rc0_release = convert_to_timezone_aware(rc0_release) if rc0_release is not None else None
            total_blocked_time = timedelta(0)
            target = target.lower()
            block_start = None  # Tracks when blocked state started
            changes_list.sort(key=lambda x: x[2], reverse=False)
            slipped_due_to_block = 0
            
            for added, removed, timestamp in changes_list:
                added_lower = added.lower()
                removed_lower = removed.lower()
                
                # Record when validation entered blocked state
                if added_lower == target:
                    block_start = timestamp

                # Calculate time spent in blocked state
                if removed_lower == target and block_start is not None:
                    time_in_blocked = timestamp - block_start
                    total_blocked_time += time_in_blocked
                    
                    # Check if blocked period spans RC0 (which causes slip)
                    if rc0_release and timestamp > rc0_release and block_start < rc0_release:
                        slipped_due_to_block = 1
                    
                    block_start = None
                
            # If still blocked at end, add time from block start to now
            if block_start is not None:
                now = datetime.now(tz=utc)
                total_blocked_time += now - block_start
            
            return total_blocked_time, slipped_due_to_block

        # Calculate blocked time per firmware
        blocked_time_by_fw = {}
        for fw, validations_dict in validation_changes.items():
            blocked_times_list = [
                sum_blocked_time(changes, rc0_releases.get(fw, None)) 
                for changes in validations_dict.values()
            ]
            blocked_times = [item[0] for item in blocked_times_list]
            slipped_list = [item[1] for item in blocked_times_list]
            
            total_blocked_time = timedelta(0)
            for time in blocked_times:
                total_blocked_time += time
            
            blocked_time_by_fw[fw] = (total_blocked_time, sum(slipped_list))

        # Aggregate time spent by firmware from bucket statements
        fw_time_spent = defaultdict(timedelta)
        for summary, time_spent in buckets:
            fw_time_spent[extract_fw(summary)] += time_spent

        # Process validations and build per-firmware metrics
        fw_dict = defaultdict(list)
        ignored_fw_dict = defaultdict(list)
        processed_ids = set()
        
        for id_readable, created, completion, first_assigned, fix_version, assignee in validations:
            # Include only validations with firmware context and assigned to TCoE
            if fix_version is not None and assignee in TCoE_MEMBERS:
                if id_readable not in processed_ids:
                    # Use creation date if no TCoE assignment found (log warning)
                    if not first_assigned:
                        first_assigned = created
                        logger.warning(f"{id_readable} has no first assignment to TCoE but is currently assigned")
                    
                    # Use current time if not yet completed
                    completion = completion if completion is not None else convert_to_timezone_aware(datetime.now())
                    
                    fw_dict[fix_version].append((completion, first_assigned))
                    processed_ids.add(id_readable)
            else:
                # Track ignored validations for debugging
                ignored_fw_dict[fix_version].append(id_readable)
                reason = "is not assigned to TCoE" if assignee not in TCoE_MEMBERS else "has no fix version"
                logger.debug(f"{id_readable}: {reason}")

        # Compute per-firmware metrics
        metrics_by_fw = {}
        for fw, validations_list in fw_dict.items():
            rc0_release = rc0_releases.get(fw, None)
            if not rc0_release:
                logger.warning(f"Firmware {fw} has validations but no RC0 release date, skipping...")
                continue
            rc0_release = convert_to_timezone_aware(rc0_release) if rc0_release is not None else None
            
            if fw not in fw_time_spent:
                logger.warning(f"Ignoring fw: {fw} because it has no time spent data")
                continue

            time_spent = fw_time_spent[fw]
            avg_time_spent = time_spent / len(validations_list)

            # Initialize metric structure
            metrics_by_fw[fw] = {
                "date": rc0_release,
                "avg_time_spent": avg_time_spent,
                "count": len(validations_list)
            }

            if not rc0_release:
                logger.warning(f"Ignoring fw: {fw} because it has no RC0 release date")
                continue

            # Calculate lifespan metrics
            lifespans = [completion - first_assigned for completion, first_assigned in validations_list]
            sum_lifespans = timedelta(0)
            for lifespan in lifespans:
                sum_lifespans += lifespan
            avg_val_lifespan = sum_lifespans / len(validations_list)

            # Calculate queue position for each validation
            completion_times = sorted([c for c, _ in validations_list])
            assignment_times = [a for _, a in validations_list]
            total_queue = 0
            
            for completion, first_assigned in validations_list:
                right_idx = bisect.bisect_right(completion_times, first_assigned)
                left_idx = bisect.bisect_left(completion_times, completion)
                queue_depth = right_idx - left_idx
                total_queue += queue_depth

            avg_queue = total_queue / len(validations_list)

            # Calculate blocked and waiting time
            blocked_time, slipped_due_to_block = blocked_time_by_fw.get(fw, (timedelta(0), 0))
            avg_blocked_time = blocked_time / len(validations_list)
            avg_waiting_time = max((avg_val_lifespan - avg_time_spent) - avg_blocked_time, timedelta(0))

            # Add computed metrics
            metrics_by_fw[fw].update({
                "time_spent_share": avg_time_spent / avg_val_lifespan if avg_val_lifespan > timedelta(0) else 0,
                "blocked_share": avg_blocked_time / avg_val_lifespan if avg_val_lifespan > timedelta(0) else 0,
                "waiting_share": avg_waiting_time / avg_val_lifespan if avg_val_lifespan > timedelta(0) else 0,
                "avg_val_lifespan": avg_val_lifespan,
                "queue": avg_queue,
            })

            # Classify validations by stage bucket
            buckets = {"pre": 0, "during": 0, "slip": 0}
            for completion, first_assigned in validations_list:
                if rc0_release is None or completion < rc0_release:
                    bucket = "pre"
                elif first_assigned is not None and first_assigned > rc0_release:
                    bucket = "slip"
                else:
                    bucket = "during"
                buckets[bucket] += 1

            # Calculate presumed overassignments (validations assigned faster than capacity)
            presump_overassignments = 0
            validations_pre_sort = sorted([a for c, a in validations_list if c < rc0_release or rc0_release is None])
            
            for idx, assignment_time in enumerate(validations_pre_sort):
                assigned_count = idx + 1
                time_available = working_hours_only_timedelta(rc0_release, assignment_time)
                adjusted_time_available = (time_available / timedelta(hours=40)) * timedelta(hours=weekly_working_hours)
                validation_capacity = adjusted_time_available * validation_time_share / avg_val_duration
                presump_overassignments = max([assigned_count - validation_capacity, presump_overassignments])

            # Adjust slip and overassignment counts
            slip_excluding_blocked = max([buckets.get('slip') - slipped_due_to_block, 0])
            presump_overassignments = min([slip_excluding_blocked, presump_overassignments])
            
            buckets['overassigned'] = presump_overassignments
            buckets['slip'] = max([slip_excluding_blocked - presump_overassignments, 0])
            buckets['blocked'] = slipped_due_to_block

            # Convert bucket counts to shares
            for bucket_name, bucket_count in buckets.items():
                bucket_share = bucket_count / len(validations_list) if len(validations_list) > 0 else 0
                metrics_by_fw[fw][bucket_name] = bucket_share

        # Format final output and cache
        okr4_data = [{"fw": fw, **metrics} for fw, metrics in metrics_by_fw.items()]
        okr4_data.sort(key=lambda x: x['date'], reverse=True)
        
        set_okr4_data(okr4_data)
        return okr4_data

