import pandas as pd
from psycopg2.extras import execute_values
from prepared_layers.utils import get_logger, POSTGRES_DB_ANALYTICS
from prepared_layers.database import get_db_connection

logger = get_logger(__name__)

def process_cdr_data_1(interval_minutes: int, table_suffix: str):
    """
    Process CDR Data - 1: Summarize CDR data by time intervals.
    
    Creates summaries with:
    - datetime: start of the summary range
    - msisdn: the msisdn the summary relates to
    - call statistics: summarized by type and volume
    - data usage: summarized by type and volume
    """
    logger.info(f"Processing CDR Data - 1 ({interval_minutes} min summaries)")
    
    conn = get_db_connection(POSTGRES_DB_ANALYTICS)
    cursor = conn.cursor()
    
    try:
        # Determine interval string for PostgreSQL
        interval_str = f'{interval_minutes} minutes'
        table_name = f'prepared_layers.cdr_usage_summary_{table_suffix}'
        
        # Get the last processed datetime
        cursor.execute(f"""
            SELECT COALESCE(MAX(datetime), '1970-01-01'::timestamp) 
            FROM {table_name}
        """)
        last_processed = cursor.fetchone()[0]
        
        # Query to aggregate voice calls
        voice_query = f"""
            SELECT 
                date_trunc('hour', start_time) + 
                    (FLOOR(EXTRACT(MINUTE FROM start_time) / {interval_minutes}) * INTERVAL '{interval_minutes} minutes') as datetime,
                msisdn,
                COUNT(CASE WHEN call_type = 'voice' THEN 1 END) as voice_call_count,
                COUNT(CASE WHEN call_type = 'video' THEN 1 END) as video_call_count,
                COALESCE(SUM(call_duration_sec), 0) as total_call_duration_sec
            FROM cdr_data.voice_calls
            WHERE start_time > %s
            GROUP BY datetime, msisdn
        """
        
        # Query to aggregate data usage
        data_query = f"""
            SELECT 
                date_trunc('hour', event_datetime) + 
                    (FLOOR(EXTRACT(MINUTE FROM event_datetime) / {interval_minutes}) * INTERVAL '{interval_minutes} minutes') as datetime,
                msisdn,
                COALESCE(SUM(CASE WHEN data_type = 'video' THEN up_bytes ELSE 0 END), 0) as video_up,
                COALESCE(SUM(CASE WHEN data_type = 'video' THEN down_bytes ELSE 0 END), 0) as video_down,
                COALESCE(SUM(CASE WHEN data_type = 'audio' THEN up_bytes ELSE 0 END), 0) as audio_up,
                COALESCE(SUM(CASE WHEN data_type = 'audio' THEN down_bytes ELSE 0 END), 0) as audio_down,
                COALESCE(SUM(CASE WHEN data_type = 'image' THEN up_bytes ELSE 0 END), 0) as image_up,
                COALESCE(SUM(CASE WHEN data_type = 'image' THEN down_bytes ELSE 0 END), 0) as image_down,
                COALESCE(SUM(CASE WHEN data_type = 'text' THEN up_bytes ELSE 0 END), 0) as text_up,
                COALESCE(SUM(CASE WHEN data_type = 'text' THEN down_bytes ELSE 0 END), 0) as text_down,
                COALESCE(SUM(CASE WHEN data_type = 'application' THEN up_bytes ELSE 0 END), 0) as app_up,
                COALESCE(SUM(CASE WHEN data_type = 'application' THEN down_bytes ELSE 0 END), 0) as app_down,
                COALESCE(SUM(up_bytes), 0) as total_up,
                COALESCE(SUM(down_bytes), 0) as total_down
            FROM cdr_data.data_usage
            WHERE event_datetime > %s
            GROUP BY datetime, msisdn
        """
        
        # Fetch voice data
        cursor.execute(voice_query, (last_processed,))
        voice_data = cursor.fetchall()
        voice_df = pd.DataFrame(voice_data, columns=['datetime', 'msisdn', 'voice_call_count', 'video_call_count', 'total_call_duration_sec'])
        
        # Fetch data usage
        cursor.execute(data_query, (last_processed,))
        data_data = cursor.fetchall()
        data_df = pd.DataFrame(data_data, columns=['datetime', 'msisdn', 'video_up', 'video_down', 'audio_up', 'audio_down', 
                                                    'image_up', 'image_down', 'text_up', 'text_down', 'app_up', 'app_down',
                                                    'total_up', 'total_down'])
        
        if voice_df.empty and data_df.empty:
            logger.info(f"No new data to process for {table_suffix}")
            return
        
        # Merge voice and data on datetime and msisdn
        if not voice_df.empty and not data_df.empty:
            merged_df = pd.merge(voice_df, data_df, on=['datetime', 'msisdn'], how='outer')
        elif not voice_df.empty:
            merged_df = voice_df
        else:
            merged_df = data_df
            
        # Fill NaN values with 0
        merged_df = merged_df.fillna(0)
        
        # Insert into the summary table
        insert_query = f"""
            INSERT INTO {table_name} (
                datetime, msisdn, voice_call_count, video_call_count, total_call_duration_sec,
                video_data_up_bytes, video_data_down_bytes, audio_data_up_bytes, audio_data_down_bytes,
                image_data_up_bytes, image_data_down_bytes, text_data_up_bytes, text_data_down_bytes,
                application_data_up_bytes, application_data_down_bytes, total_up_bytes, total_down_bytes
            ) VALUES %s
            ON CONFLICT (datetime, msisdn) DO UPDATE SET
                voice_call_count = EXCLUDED.voice_call_count,
                video_call_count = EXCLUDED.video_call_count,
                total_call_duration_sec = EXCLUDED.total_call_duration_sec,
                video_data_up_bytes = EXCLUDED.video_data_up_bytes,
                video_data_down_bytes = EXCLUDED.video_data_down_bytes,
                audio_data_up_bytes = EXCLUDED.audio_data_up_bytes,
                audio_data_down_bytes = EXCLUDED.audio_data_down_bytes,
                image_data_up_bytes = EXCLUDED.image_data_up_bytes,
                image_data_down_bytes = EXCLUDED.image_data_down_bytes,
                text_data_up_bytes = EXCLUDED.text_data_up_bytes,
                text_data_down_bytes = EXCLUDED.text_data_down_bytes,
                application_data_up_bytes = EXCLUDED.application_data_up_bytes,
                application_data_down_bytes = EXCLUDED.application_data_down_bytes,
                total_up_bytes = EXCLUDED.total_up_bytes,
                total_down_bytes = EXCLUDED.total_down_bytes
        """
        
        records = []
        for _, row in merged_df.iterrows():
            records.append((
                row['datetime'], row['msisdn'],
                int(row.get('voice_call_count', 0)), int(row.get('video_call_count', 0)), 
                int(row.get('total_call_duration_sec', 0)),
                int(row.get('video_up', 0)), int(row.get('video_down', 0)),
                int(row.get('audio_up', 0)), int(row.get('audio_down', 0)),
                int(row.get('image_up', 0)), int(row.get('image_down', 0)),
                int(row.get('text_up', 0)), int(row.get('text_down', 0)),
                int(row.get('app_up', 0)), int(row.get('app_down', 0)),
                int(row.get('total_up', 0)), int(row.get('total_down', 0))
            ))
        
        if records:
            execute_values(cursor, insert_query, records)
            conn.commit()
            logger.info(f"Inserted {len(records)} records into {table_name}")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error processing CDR Data - 1 ({table_suffix}): {e}")
    finally:
        cursor.close()
        conn.close()


def process_cdr_data_2():
    """
    Process CDR Data - 2: User tower sessions.
    
    Tracks sessions when an msisdn has > 2 consecutive interactions with a tower_id.
    Creates records with:
    - msisdn
    - tower_id
    - session_start
    - session_end
    """
    logger.info("Processing CDR Data - 2 (Tower Sessions)")
    
    conn = get_db_connection(POSTGRES_DB_ANALYTICS)
    cursor = conn.cursor()
    
    try:
        # Clear existing sessions and recalculate (simpler approach)
        cursor.execute("TRUNCATE TABLE prepared_layers.cdr_tower_sessions")
        
        # Get all interactions ordered by msisdn, tower_id, and time
        # Combine voice and data interactions
        query = """
            WITH all_interactions AS (
                SELECT msisdn, tower_id, event_datetime as interaction_time
                FROM cdr_data.data_usage
                WHERE tower_id IS NOT NULL
                UNION ALL
                SELECT msisdn, tower_id, start_time as interaction_time
                FROM cdr_data.voice_calls
                WHERE tower_id IS NOT NULL
            ),
            ordered_interactions AS (
                SELECT 
                    msisdn, 
                    tower_id, 
                    interaction_time,
                    LAG(tower_id) OVER (PARTITION BY msisdn ORDER BY interaction_time) as prev_tower,
                    LAG(interaction_time) OVER (PARTITION BY msisdn ORDER BY interaction_time) as prev_time
                FROM all_interactions
                ORDER BY msisdn, interaction_time
            ),
            session_boundaries AS (
                SELECT 
                    msisdn,
                    tower_id,
                    interaction_time,
                    CASE 
                        WHEN prev_tower IS NULL OR prev_tower != tower_id 
                        THEN 1 
                        ELSE 0 
                    END as is_new_session
                FROM ordered_interactions
            ),
            session_groups AS (
                SELECT 
                    msisdn,
                    tower_id,
                    interaction_time,
                    SUM(is_new_session) OVER (PARTITION BY msisdn ORDER BY interaction_time) as session_id
                FROM session_boundaries
            ),
            session_stats AS (
                SELECT 
                    msisdn,
                    tower_id,
                    session_id,
                    MIN(interaction_time) as session_start,
                    MAX(interaction_time) as session_end,
                    COUNT(*) as interaction_count
                FROM session_groups
                GROUP BY msisdn, tower_id, session_id
                HAVING COUNT(*) > 2
            )
            SELECT msisdn, tower_id, session_start, session_end, interaction_count
            FROM session_stats
            ORDER BY msisdn, session_start
        """
        
        cursor.execute(query)
        sessions = cursor.fetchall()
        
        if sessions:
            insert_query = """
                INSERT INTO prepared_layers.cdr_tower_sessions 
                (msisdn, tower_id, session_start, session_end, interaction_count)
                VALUES %s
            """
            execute_values(cursor, insert_query, sessions)
            conn.commit()
            logger.info(f"Inserted {len(sessions)} tower sessions")
        else:
            logger.info("No tower sessions found")
            conn.commit()
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error processing CDR Data - 2: {e}")
    finally:
        cursor.close()
        conn.close()
