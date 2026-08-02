import streamlit as st
import pandas as pd
import json

st.set_page_config(page_title="LogSense AI", layout="wide")

session = st.connection("snowflake").session()

SEARCH_SERVICE = "KAFKA_LOGS.RAW.LOG_SEARCH_SERVICE"

# --- Sidebar: Log Ingestion ---
with st.sidebar:
    st.header("Log Ingestion")
    uploaded_files = st.file_uploader(
        "Upload log files",
        type=["log", "txt", "csv"],
        accept_multiple_files=True
    )
    app_name = st.text_input("App Name", placeholder="e.g. kafka, nginx, postgresql")

    if st.button("Ingest & Parse", disabled=not uploaded_files or not app_name):
        import time
        total_rows = 0
        total_files = len(uploaded_files)
        escaped_app = app_name.replace("'", "''")

        progress_bar = st.progress(0)
        status_text = st.empty()
        time_text = st.empty()
        start_time = time.time()

        for idx, file in enumerate(uploaded_files):
            file_name = file.name
            status_text.info(f"Processing file {idx + 1}/{total_files}: **{file_name}**...")
            elapsed = time.time() - start_time
            time_text.caption(f"Elapsed: {int(elapsed)}s")

            content = file.read().decode("utf-8", errors="replace")
            line_count = content.count('\n') + 1
            status_text.info(
                f"Processing file {idx + 1}/{total_files}: **{file_name}** "
                f"({line_count} lines) — Parsing with AI..."
            )

            escaped_content = content.replace("'", "''")
            result = session.sql(
                f"CALL KAFKA_LOGS.RAW.PARSE_ANY_LOG('{escaped_content}', '{escaped_app}')"
            ).to_pandas()
            row_count = result.iloc[0, 0]
            parsed = int(row_count.split()[0]) if row_count else 0
            total_rows += parsed

            progress_bar.progress((idx + 1) / total_files)
            elapsed = time.time() - start_time
            time_text.caption(f"Elapsed: {int(elapsed)}s")

        # Done
        elapsed = time.time() - start_time
        status_text.empty()
        time_text.empty()
        progress_bar.empty()

        st.success(
            f"Done! Ingested **{total_rows}** rows from "
            f"**{total_files}** file(s) in {int(elapsed)}s"
        )
        st.balloons()

        preview = session.sql(
            f"SELECT APP_NAME, TIMESTAMP, LOG_LEVEL, COMPONENT, MESSAGE "
            f"FROM KAFKA_LOGS.RAW.PARSED_LOGS "
            f"WHERE APP_NAME = '{escaped_app}' "
            f"ORDER BY INGESTED_AT DESC LIMIT 10"
        ).to_pandas()
        st.caption("Preview of recently parsed rows:")
        st.dataframe(preview, use_container_width=True)

    # --- Sidebar: Manage Logs ---
    st.divider()
    st.header("Manage Logs")

    @st.cache_data(ttl=30)
    def get_log_files():
        df = session.sql(
            "SELECT APP_NAME, SOURCE_FILE, COUNT(*) AS ROW_COUNT, "
            "MIN(INGESTED_AT) AS INGESTED_AT "
            "FROM KAFKA_LOGS.RAW.PARSED_LOGS "
            "GROUP BY APP_NAME, SOURCE_FILE "
            "ORDER BY INGESTED_AT DESC"
        ).to_pandas()
        return df

    log_files_df = get_log_files()

    if log_files_df.empty:
        st.caption("No log files ingested yet.")
    else:
        options = {
            f"{row['APP_NAME']} - {row['SOURCE_FILE']} ({row['ROW_COUNT']} rows)": {
                "app_name": row["APP_NAME"],
                "source_file": row["SOURCE_FILE"],
                "row_count": row["ROW_COUNT"]
            }
            for _, row in log_files_df.iterrows()
        }

        selected = st.multiselect("Select log files to delete", list(options.keys()))

        if selected:
            total_rows_selected = sum(options[s]["row_count"] for s in selected)
            st.warning(
                f"Are you sure you want to delete **{len(selected)}** file(s) "
                f"(**{total_rows_selected}** total rows)?"
            )
            confirm = st.checkbox("Yes, I confirm deletion")

            if st.button("Delete Selected", type="primary", disabled=not confirm):
                conditions = " OR ".join(
                    f"(APP_NAME = '{options[s]['app_name'].replace(chr(39), chr(39)+chr(39))}' "
                    f"AND SOURCE_FILE = '{options[s]['source_file'].replace(chr(39), chr(39)+chr(39))}')"
                    for s in selected
                )
                session.sql(
                    f"DELETE FROM KAFKA_LOGS.RAW.PARSED_LOGS WHERE {conditions}"
                ).collect()
                st.success(f"Deleted {total_rows_selected} rows from {len(selected)} file(s).")
                st.cache_data.clear()
                st.rerun()

# --- Main Area ---
st.title("LogSense AI (Diagnosis and Analytics)")

# App name filter
@st.cache_data(ttl=60)
def get_app_names():
    df = session.sql(
        "SELECT DISTINCT APP_NAME FROM KAFKA_LOGS.RAW.PARSED_LOGS ORDER BY APP_NAME"
    ).to_pandas()
    return ["All Apps"] + df["APP_NAME"].tolist()

app_filter = st.selectbox("Filter by App", get_app_names())

# Search input
search_query = st.text_input(
    "Search logs (natural language)",
    placeholder="e.g. connection refused, rebalance failure, broker not available"
)

def search_logs(query, app_name_filter=None, limit=50):
    search_params = {
        "query": query,
        "columns": ["MESSAGE", "APP_NAME", "LOG_LEVEL", "COMPONENT"],
        "limit": limit
    }
    if app_name_filter and app_name_filter != "All Apps":
        search_params["filter"] = {"@eq": {"APP_NAME": app_name_filter}}

    params_json = json.dumps(search_params)
    result = session.sql(f"""
        SELECT PARSE_JSON(SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
            '{SEARCH_SERVICE}',
            $${params_json}$$
        )) AS results
    """).to_pandas()
    parsed = json.loads(result["RESULTS"].iloc[0])
    return parsed.get("results", [])

def call_ai_complete(system_prompt, user_content):
    models = ["mistral-large2", "llama3.1-70b", "claude-3-5-sonnet"]
    for model in models:
        try:
            result = session.sql(f"""
                SELECT SNOWFLAKE.CORTEX.AI_COMPLETE(
                    '{model}',
                    [
                        {{'role': 'system', 'content': $${system_prompt}$$}},
                        {{'role': 'user', 'content': $${user_content}$$}}
                    ],
                    {{}}
                ) AS response
            """).to_pandas()
            return result["RESPONSE"].iloc[0]
        except Exception:
            continue
    return None

def classify_severity(message):
    msg = message.lower() if message else ""
    if any(k in msg for k in ["fatal", "oom", "out of memory", "kill", "crash", "corrupt"]):
        return "Critical"
    if any(k in msg for k in ["connection refused", "timeout", "unreachable", "failed to connect", "broker not available"]):
        return "High"
    if any(k in msg for k in ["config", "invalid", "not found", "missing", "permission denied"]):
        return "Medium"
    return "Low"

# Search and display
if search_query:
    with st.spinner("Searching logs..."):
        results = search_logs(search_query, app_filter)

    if not results:
        st.warning("No matching logs found.")
    else:
        logs_df = pd.DataFrame(results)
        logs_df = logs_df.drop(columns=["@scores"], errors="ignore")
        logs_df.columns = [c.upper() for c in logs_df.columns]

        st.subheader(f"Matching Log Entries ({len(logs_df)} results)")
        logs_df["SEVERITY"] = logs_df["MESSAGE"].apply(classify_severity)

        # Sort by severity: Critical first, then High, Medium, Low
        severity_order_map = {"Critical": 0, "High": 1, "Medium": 2, "Low": 3}
        logs_df["_SEV_SORT"] = logs_df["SEVERITY"].map(severity_order_map)
        logs_df = logs_df.sort_values("_SEV_SORT").drop(columns=["_SEV_SORT"]).reset_index(drop=True)

        # Pagination
        page_size = 10
        total_pages = max(1, (len(logs_df) + page_size - 1) // page_size)
        page = st.selectbox("Page", range(1, total_pages + 1), format_func=lambda x: f"Page {x} of {total_pages}")
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        st.dataframe(logs_df.iloc[start_idx:end_idx], use_container_width=True)
        st.caption(f"Showing rows {start_idx + 1}-{min(end_idx, len(logs_df))} of {len(logs_df)}")

        # Severity breakdown chart
        st.subheader("Severity Breakdown")
        severity_counts = logs_df.groupby("SEVERITY").size().reset_index(name="Count")
        severity_order = ["Critical", "High", "Medium", "Low"]
        severity_counts["SEVERITY"] = pd.Categorical(
            severity_counts["SEVERITY"], categories=severity_order, ordered=True
        )
        severity_counts = severity_counts.sort_values("SEVERITY").set_index("SEVERITY")
        st.bar_chart(severity_counts)

        # Diagnose button
        if st.button("Diagnose", type="primary"):
            log_lines = "\n".join(
                f"[{row.get('LOG_LEVEL', 'N/A')}] [{row.get('COMPONENT', '')}] {row.get('MESSAGE', '')}"
                for _, row in logs_df.iterrows()
            )

            analysis_prompt = (
                "You are an infrastructure and DevOps expert. "
                "Analyze these log entries and return your analysis in EXACTLY this format:\n\n"
                "ROOT CAUSE: <one clear sentence>\n"
                "SEVERITY: <critical/high/medium/low>\n"
                "IMPACT: <what this affects>\n"
                "FIX STEPS:\n1. <step>\n2. <step>\n3. <step>\n"
                "PREVENTION: <how to prevent>\n\n"
                "Be specific and actionable."
            )
            user_content = f"User's search: {search_query}\n\nMatching log entries:\n{log_lines}"

            with st.spinner("Running AI diagnosis..."):
                response = call_ai_complete(analysis_prompt, user_content)

            st.divider()
            st.subheader("Diagnosis Report")

            if response:
                # Replace literal \n with actual newlines
                cleaned = response.replace("\\n", "\n")
                # Show AI response line by line
                for line in cleaned.strip().split("\n"):
                    line = line.strip()
                    if line:
                        st.write(line)
            else:
                # AI unavailable - show local pattern-based diagnosis
                st.caption("(AI unavailable on trial account - showing pattern-based analysis)")

                # Build diagnosis from log patterns
                error_count = len(logs_df[logs_df.get("LOG_LEVEL", pd.Series()) == "ERROR"]) if "LOG_LEVEL" in logs_df.columns else 0
                warn_count = len(logs_df[logs_df.get("LOG_LEVEL", pd.Series()) == "WARN"]) if "LOG_LEVEL" in logs_df.columns else 0
                components = logs_df["COMPONENT"].unique().tolist() if "COMPONENT" in logs_df.columns else []

                # Determine severity from log levels
                if error_count > 5:
                    sev = "CRITICAL"
                elif error_count > 0:
                    sev = "HIGH"
                elif warn_count > 5:
                    sev = "MEDIUM"
                else:
                    sev = "LOW"

                # Build report as a table
                report_data = []
                report_data.append({"Field": "Search Query", "Details": search_query})
                report_data.append({"Field": "Matching Logs", "Details": f"{len(logs_df)} entries found"})
                report_data.append({"Field": "Severity", "Details": sev})
                report_data.append({"Field": "Error Count", "Details": str(error_count)})
                report_data.append({"Field": "Warning Count", "Details": str(warn_count)})
                report_data.append({"Field": "Components", "Details": ", ".join(components[:5])})

                # Find most common error pattern
                if "MESSAGE" in logs_df.columns:
                    messages = logs_df["MESSAGE"].tolist()
                    # Get unique short patterns
                    patterns = []
                    for msg in messages[:10]:
                        short = msg[:100] if msg else ""
                        if short and short not in patterns:
                            patterns.append(short)

                    for i, p in enumerate(patterns[:3]):
                        report_data.append({"Field": f"Pattern {i+1}", "Details": p})

                report_df = pd.DataFrame(report_data)
                st.table(report_df)
