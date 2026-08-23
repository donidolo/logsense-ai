import streamlit as st
import pandas as pd
import json
import re
\N
st.set_page_config(page_title="LogSense AI", layout="wide")
\N
session = st.connection("snowflake").session()
\N
SEARCH_SERVICE = "KAFKA_LOGS.RAW.LOG_SEARCH_SERVICE"
\N
# --- Sidebar ---
with st.sidebar:
    st.header("Log Ingestion")
    uploaded_files = st.file_uploader(
        "Upload log files",
        type=["log", "txt", "csv"],
        accept_multiple_files=True
    )
\N
    # --- Sidebar: Service Registry ---
    st.divider()
    st.header("Service Registry")
\N
    def auto_fill_defaults(service_name):
        name_lower = service_name.lower() if service_name else ""
        if any(k in name_lower for k in ["kafka", "broker", "connect", "zookeeper"]):
            return {"tier": "Tier-1", "team": "Platform Team", "function": "Transaction Processing", "customers": "All paying customers"}
        elif any(k in name_lower for k in ["nginx", "haproxy", "gateway", "ingress", "lb"]):
            return {"tier": "Tier-1", "team": "Platform Team", "function": "API Gateway", "customers": "All users"}
        elif any(k in name_lower for k in ["auth", "login", "oauth", "sso", "identity"]):
            return {"tier": "Tier-1", "team": "Security Team", "function": "User Authentication", "customers": "All users"}
        elif any(k in name_lower for k in ["payment", "billing", "stripe", "checkout"]):
            return {"tier": "Tier-1", "team": "Backend Team", "function": "Payment Processing", "customers": "All paying customers"}
        elif any(k in name_lower for k in ["openstack", "cloud", "nova", "neutron", "cinder"]):
            return {"tier": "Tier-2", "team": "DevOps Team", "function": "Cloud Infrastructure", "customers": "Internal"}
        elif any(k in name_lower for k in ["systemd", "cron", "init", "supervisor"]):
            return {"tier": "Tier-2", "team": "Platform Team", "function": "System Services", "customers": "All users"}
        elif any(k in name_lower for k in ["monitor", "prometheus", "grafana", "alert"]):
            return {"tier": "Tier-2", "team": "SRE Team", "function": "Observability", "customers": "Internal"}
        elif any(k in name_lower for k in ["log", "elastic", "fluentd", "splunk"]):
            return {"tier": "Tier-3", "team": "Platform Team", "function": "Log Management", "customers": "Internal"}
        else:
            return {"tier": "Tier-2", "team": "Engineering Team", "function": "General Service", "customers": "Internal"}
\N
    sr_service_name = st.text_input("Service Name (matches COMPONENT)", placeholder="e.g. openstack, kafka-server-start.sh")
\N
    defaults = auto_fill_defaults(sr_service_name)
\N
    with st.form("service_registry_form"):
        st.caption("Leave fields empty to use auto-generated defaults.")
        sr_tier = st.selectbox("Tier", ["Tier-1", "Tier-2", "Tier-3"], index=["Tier-1", "Tier-2", "Tier-3"].index(defaults["tier"]))
        sr_team_owner = st.text_input("Team Owner", placeholder="e.g. Platform Team")
        sr_business_function = st.text_input("Business Function", placeholder="e.g. Payment Processing")
        sr_affected_customers = st.text_input("Affected Customers", placeholder="e.g. All, Premium Only, Internal")
        sr_submitted = st.form_submit_button("Save Service")
\N
    if sr_submitted and sr_service_name:
        final_team = sr_team_owner.strip() if sr_team_owner.strip() else defaults["team"]
        final_func = sr_business_function.strip() if sr_business_function.strip() else defaults["function"]
        final_cust = sr_affected_customers.strip() if sr_affected_customers.strip() else defaults["customers"]
\N
        escaped_name = sr_service_name.replace("'", "''")
        escaped_team = final_team.replace("'", "''")
        escaped_func = final_func.replace("'", "''")
        escaped_cust = final_cust.replace("'", "''")
        session.sql(f"""
            MERGE INTO KAFKA_LOGS.RAW.SERVICE_REGISTRY t
            USING (SELECT '{escaped_name}' AS SERVICE_NAME) s
            ON UPPER(t.SERVICE_NAME) = UPPER(s.SERVICE_NAME)
            WHEN MATCHED THEN UPDATE SET
                TIER = '{sr_tier}',
                TEAM_OWNER = '{escaped_team}',
                BUSINESS_FUNCTION = '{escaped_func}',
                AFFECTED_CUSTOMERS = '{escaped_cust}'
            WHEN NOT MATCHED THEN INSERT (SERVICE_NAME, TIER, TEAM_OWNER, BUSINESS_FUNCTION, AFFECTED_CUSTOMERS)
            VALUES ('{escaped_name}', '{sr_tier}', '{escaped_team}', '{escaped_func}', '{escaped_cust}')
        """).collect()
        st.success(f"Service '{sr_service_name}' saved! (Team: {final_team}, Function: {final_func}, Customers: {final_cust})")
        st.cache_data.clear()
\N
    # --- Sidebar: App Name (Ingest & Parse) ---
    st.divider()
    app_name = st.text_input("App Name", placeholder="e.g. kafka, nginx, postgresql")
\N
    if st.button("Ingest & Parse", disabled=not uploaded_files or not app_name):
        import time
        total_rows = 0
        total_files = len(uploaded_files)
        escaped_app = app_name.replace("'", "''")
\N
        progress_bar = st.progress(0)
        status_text = st.empty()
        time_text = st.empty()
        start_time = time.time()
\N
        for idx, file in enumerate(uploaded_files):
            file_name = file.name
            status_text.info(f"Processing file {idx + 1}/{total_files}: **{file_name}**...")
            elapsed = time.time() - start_time
            time_text.caption(f"Elapsed: {int(elapsed)}s")
\N
            content = file.read().decode("utf-8", errors="replace")
            line_count = content.count('\n') + 1
            status_text.info(
                f"Processing file {idx + 1}/{total_files}: **{file_name}** "
                f"({line_count} lines) — Parsing with AI..."
            )
\N
            escaped_content = content.replace("'", "''")
            result = session.sql(
                f"CALL KAFKA_LOGS.RAW.PARSE_ANY_LOG('{escaped_content}', '{escaped_app}')"
            ).to_pandas()
            row_count = result.iloc[0, 0]
            parsed = int(row_count.split()[0]) if row_count else 0
            total_rows += parsed
\N
            progress_bar.progress((idx + 1) / total_files)
            elapsed = time.time() - start_time
            time_text.caption(f"Elapsed: {int(elapsed)}s")
\N
        elapsed = time.time() - start_time
        status_text.empty()
        time_text.empty()
        progress_bar.empty()
\N
        st.success(
            f"Done! Ingested **{total_rows}** rows from "
            f"**{total_files}** file(s) in {int(elapsed)}s"
        )
        st.balloons()
\N
        preview = session.sql(
            f"SELECT APP_NAME, TIMESTAMP, LOG_LEVEL, COMPONENT, MESSAGE "
            f"FROM KAFKA_LOGS.RAW.PARSED_LOGS "
            f"WHERE APP_NAME = '{escaped_app}' "
            f"ORDER BY INGESTED_AT DESC LIMIT 10"
        ).to_pandas()
        st.caption("Preview of recently parsed rows:")
        st.dataframe(preview, use_container_width=True)
\N
    # --- Sidebar: Manage Logs ---
    st.divider()
    st.header("Manage Logs")
\N
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
\N
    log_files_df = get_log_files()
\N
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
\N
        selected = st.multiselect("Select log files to delete", list(options.keys()))
\N
        if selected:
            total_rows_selected = sum(options[s]["row_count"] for s in selected)
            st.warning(
                f"Are you sure you want to delete **{len(selected)}** file(s) "
                f"(**{total_rows_selected}** total rows)?"
            )
            confirm = st.checkbox("Yes, I confirm deletion")
\N
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
\N
    # --- Sidebar: Incident Tracker ---
    st.divider()
    st.header("Incident Tracker")
\N
    @st.cache_data(ttl=15)
    def get_open_incidents():
        df = session.sql("""
            SELECT TICKET_ID, TITLE, COMPONENT, STATUS, SEVERITY, PRIORITY, CREATED_AT
            FROM KAFKA_LOGS.RAW.INCIDENT_TICKETS
            ORDER BY CREATED_AT DESC
            LIMIT 20
        """).to_pandas()
        return df
\N
    incidents_df = get_open_incidents()
\N
    if incidents_df.empty:
        st.caption("No incidents yet. Create one from the Diagnosis panel.")
    else:
        open_count = len(incidents_df[incidents_df["STATUS"] == "Open"])
        in_progress_count = len(incidents_df[incidents_df["STATUS"] == "In Progress"])
        resolved_count = len(incidents_df[incidents_df["STATUS"].isin(["Resolved", "Closed"])])
        st.markdown(f"🔴 {open_count} Open  |  🟡 {in_progress_count} In Progress  |  🟢 {resolved_count} Resolved")
\N
        with st.expander("Recent Incidents", expanded=False):
            for _, row in incidents_df.iterrows():
                ticket_id = int(row["TICKET_ID"])
                priority = row["PRIORITY"]
                component = row.get("COMPONENT", "unknown") or "unknown"
                status = row["STATUS"]
                created = row["CREATED_AT"]
\N
                # Format date
                if created and not pd.isna(created):
                    if hasattr(created, 'strftime'):
                        created_str = created.strftime("%b %d, %H:%M")
                    else:
                        created_str = str(created)[:12]
                else:
                    created_str = "N/A"
\N
                # Clean title
                raw_title = row["TITLE"] if row["TITLE"] else ""
                clean_title = raw_title.replace('"', '').replace("ROOT CAUSE:", "").strip()
                # Remove leading [Severity] [Component] prefix if present
                clean_title = re.sub(r'^\[.*?\]\s*\[.*?\]\s*-?\s*', '', clean_title).strip()
                if len(clean_title) > 60:
                    clean_title = clean_title[:60] + "..."
\N
                # Priority badge
                priority_colors = {"P1": "🔴", "P2": "🟠", "P3": "🟡", "P4": "⚪"}
                badge = priority_colors.get(priority, "⚪")
\N
                st.markdown(f"**#{ticket_id} · {badge} {priority} · {component} · {status}**")
                if clean_title:
                    st.caption(f"{clean_title}")
                st.caption(f"Created: {created_str}")
\N
                new_status = st.selectbox(
                    "Status",
                    ["Open", "In Progress", "Resolved", "Closed"],
                    index=["Open", "In Progress", "Resolved", "Closed"].index(status),
                    key=f"status_{ticket_id}",
                    label_visibility="collapsed"
                )
\N
                if new_status != status:
                    if new_status == "Resolved":
                        res_notes = st.text_area("Resolution notes", key=f"res_{ticket_id}")
                        if st.button("Save & Resolve", key=f"resolve_{ticket_id}"):
                            escaped_notes = res_notes.replace("'", "''")
                            session.sql(f"UPDATE KAFKA_LOGS.RAW.INCIDENT_TICKETS SET STATUS = 'Resolved' WHERE TICKET_ID = {ticket_id}").collect()
                            session.sql(f"""
                                INSERT INTO KAFKA_LOGS.RAW.AUDIT_RESOLUTIONS (TICKET_ID, RESOLUTION_NOTES, COMPONENT)
                                VALUES ({ticket_id}, '{escaped_notes}', '{component.replace(chr(39), chr(39)+chr(39))}')
                            """).collect()
                            st.success("Resolved!")
                            st.cache_data.clear()
                            st.rerun()
                    else:
                        if st.button("Update Status", key=f"update_{ticket_id}"):
                            escaped_status = new_status.replace("'", "''")
                            session.sql(f"UPDATE KAFKA_LOGS.RAW.INCIDENT_TICKETS SET STATUS = '{escaped_status}' WHERE TICKET_ID = {ticket_id}").collect()
                            st.success(f"Updated to {new_status}")
                            st.cache_data.clear()
                            st.rerun()
                st.divider()
\N
# --- Main Area ---
title_col, chat_col = st.columns([8, 1])
with title_col:
    st.title("LogSense AI (Diagnosis and Analytics)")
with chat_col:
    st.write("")
    if st.button("💬 AI Chat Assistant", key="toggle_chat"):
        st.session_state["show_chat"] = not st.session_state.get("show_chat", False)
\N
# --- AI Chat Assistant ---
CHAT_SYSTEM_PROMPT = (
    "You are LogSense AI, an expert log diagnosis assistant. You have "
    "access to log data from multiple applications stored in Snowflake. "
    "When users ask about errors or issues, search the logs, analyze "
    "patterns, and provide clear root cause analysis with actionable "
    "fix recommendations. Always mention the service tier and business "
    "impact when relevant. Be concise but thorough."
)
\N
if "chat_messages" not in st.session_state:
    st.session_state["chat_messages"] = []
\N
# Layout: dashboard left, chat right (when active)
if st.session_state.get("show_chat", False):
    dashboard_col, chat_panel_col = st.columns([3, 2])
else:
    dashboard_col = st.container()
    chat_panel_col = None
\N
# Chat panel (right side)
if chat_panel_col is not None:
    with chat_panel_col:
        st.markdown("""
        <style>
        [data-testid="stHorizontalBlock"] > div:last-child h3 {
            margin-top: 0;
            padding-top: 0;
        }
        </style>
        """, unsafe_allow_html=True)
        with st.container(border=True, height=900):
            st.subheader("💬 AI Chat Assistant")
\N
            pending = st.session_state.pop("chat_pending_input", None)
            user_input = st.chat_input("Ask me anything about your logs...") or pending
\N
            if user_input:
                st.session_state["chat_messages"].append({"role": "user", "content": user_input})
\N
            chat_container = st.container(height=750, border=False)
            with chat_container:
                if not st.session_state["chat_messages"] and not user_input:
                    st.markdown('<p style="text-align: center; color: #9CA3AF; margin-top: 200px;">Ask me anything about your logs to get started</p>', unsafe_allow_html=True)
                else:
                    for msg in st.session_state["chat_messages"]:
                        with st.chat_message(msg["role"]):
                            st.markdown(msg["content"])
                            if msg.get("logs_preview"):
                                st.caption(f"Related log entries ({msg['logs_count']} found):")
                                st.dataframe(pd.DataFrame(msg["logs_preview"]), use_container_width=True, height=100)
\N
                if user_input:
                    with st.chat_message("assistant"):
                        with st.spinner("Analyzing logs..."):
                            search_params = {
                                "query": user_input,
                                "columns": ["MESSAGE", "APP_NAME", "LOG_LEVEL", "COMPONENT"],
                                "limit": 20
                            }
                            params_json = json.dumps(search_params)
                            try:
                                search_result = session.sql(f"""
                                    SELECT PARSE_JSON(SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                                        '{SEARCH_SERVICE}',
                                        $${params_json}$$
                                    )) AS results
                                """).to_pandas()
                                parsed = json.loads(search_result["RESULTS"].iloc[0])
                                log_results = parsed.get("results", [])
                            except Exception:
                                log_results = []
\N
                            context_info = ""
                            if log_results:
                                components_found = list(set(r.get("COMPONENT", "") for r in log_results if r.get("COMPONENT")))
                                for comp in components_found[:3]:
                                    escaped_c = comp.replace("'", "''")
                                    svc_df = session.sql(f"""
                                        SELECT TIER, TEAM_OWNER, BUSINESS_FUNCTION, AFFECTED_CUSTOMERS
                                        FROM KAFKA_LOGS.RAW.SERVICE_REGISTRY
                                        WHERE UPPER(SERVICE_NAME) = UPPER('{escaped_c}')
                                           OR UPPER('{escaped_c}') LIKE '%' || UPPER(SERVICE_NAME) || '%'
                                           OR UPPER(SERVICE_NAME) LIKE '%' || UPPER('{escaped_c}') || '%'
                                        LIMIT 1
                                    """).to_pandas()
                                    if not svc_df.empty:
                                        row = svc_df.iloc[0]
                                        context_info += f"Service '{comp}': {row['TIER']}, {row['BUSINESS_FUNCTION']}, affects {row['AFFECTED_CUSTOMERS']}, owned by {row['TEAM_OWNER']}.\n"
\N
                            log_summary = ""
                            if log_results:
                                log_lines = "\n".join(
                                    f"[{r.get('LOG_LEVEL','N/A')}] [{r.get('COMPONENT','')}] {r.get('MESSAGE','')}"
                                    for r in log_results[:15]
                                )
                                log_summary = f"\n\nMatching log entries ({len(log_results)} found):\n{log_lines}"
\N
                            history_context = ""
                            if len(st.session_state["chat_messages"]) > 1:
                                recent = st.session_state["chat_messages"][-6:]
                                history_context = "\n\nConversation history:\n" + "\n".join(
                                    f"{m['role']}: {m['content'][:200]}" for m in recent
                                )
\N
                            full_system = CHAT_SYSTEM_PROMPT + (f"\n\nService context:\n{context_info}" if context_info else "")
                            full_user = user_input + log_summary + history_context
\N
                            models = ["llama3.3-70b", "llama3.1-70b"]
                            ai_response = None
                            for model in models:
                                try:
                                    res = session.sql(f"""
                                        SELECT SNOWFLAKE.CORTEX.AI_COMPLETE(
                                            '{model}',
                                            [
                                                {{'role': 'system', 'content': $${full_system}$$}},
                                                {{'role': 'user', 'content': $${full_user}$$}}
                                            ],
                                            {{}}
                                        ) AS response
                                    """).to_pandas()
                                    ai_response = res["RESPONSE"].iloc[0]
                                    break
                                except Exception:
                                    continue
\N
                            if ai_response:
                                cleaned_response = ai_response.replace("\n", "\n")
                                st.markdown(cleaned_response)
\N
                                if log_results:
                                    st.caption(f"Related log entries ({len(log_results)} found):")
                                    st.dataframe(pd.DataFrame(log_results).drop(columns=["@scores"], errors="ignore"), use_container_width=True, height=150)
\N
                                st.session_state["chat_messages"].append({
                                    "role": "assistant",
                                    "content": cleaned_response,
                                    "logs_preview": log_results[:5] if log_results else None,
                                    "logs_count": len(log_results) if log_results else 0
                                })
                            else:
                                fallback_msg = "I couldn't connect to the AI service right now. Please try again."
                                st.warning(fallback_msg)
                                st.session_state["chat_messages"].append({"role": "assistant", "content": fallback_msg})
\N
# ============================================================
# DASHBOARD (in left column or full width)
# ============================================================
with dashboard_col:
\N
    # --- PART 1: Metric Cards ---
    @st.cache_data(ttl=10)
    def get_metrics():
        df = session.sql("""
            SELECT
                COUNT(CASE WHEN LOG_LEVEL IN ('ERROR','FATAL','CRIT') THEN 1 END) AS TOTAL_ERRORS,
                COUNT(DISTINCT APP_NAME) AS APPS_MONITORED,
                COUNT(CASE WHEN LOG_LEVEL IN ('FATAL','CRIT') THEN 1 END) AS CRITICAL_ISSUES,
                MAX(INGESTED_AT) AS LATEST_INGESTION
            FROM KAFKA_LOGS.RAW.PARSED_LOGS
        """).to_pandas()
        return df.iloc[0]
    
    metrics = get_metrics()
    
    from datetime import datetime
    
    def relative_time(ts):
        if ts is None or pd.isna(ts):
            return "N/A"
        now = datetime.now()
        if hasattr(ts, 'to_pydatetime'):
            ts = ts.to_pydatetime()
        diff = now - ts
        seconds = int(diff.total_seconds())
        if seconds < 60:
            return f"{seconds}s ago"
        elif seconds < 3600:
            return f"{seconds // 60}m ago"
        elif seconds < 86400:
            return f"{seconds // 3600}h ago"
        else:
            return f"{seconds // 86400}d ago"
    
    mc1, mc2, mc3, mc4 = st.columns(4)
    
    metric_card_css = """
    <style>
    div[data-testid="stMetric"] {
        background-color: #FAF9F6;
        padding: 15px;
        border-radius: 8px;
        height: 100%;
        overflow: hidden;
    }
    div[data-testid="column"]:nth-child(1) div[data-testid="stMetric"] {
        border-left: 4px solid #EF4444;
    }
    div[data-testid="column"]:nth-child(2) div[data-testid="stMetric"] {
        border-left: 4px solid #3B82F6;
    }
    div[data-testid="column"]:nth-child(3) div[data-testid="stMetric"] {
        border-left: 4px solid #F59E0B;
    }
    div[data-testid="column"]:nth-child(4) div[data-testid="stMetric"] {
        border-left: 4px solid #10B981;
    }
    </style>
    """
    st.markdown(metric_card_css, unsafe_allow_html=True)
    
    with mc1:
        st.container(border=True).metric("Total Errors", int(metrics["TOTAL_ERRORS"]))
    with mc2:
        st.container(border=True).metric("Applications Monitored", int(metrics["APPS_MONITORED"]))
    with mc3:
        st.container(border=True).metric("Critical Issues", int(metrics["CRITICAL_ISSUES"]))
    with mc4:
        st.container(border=True).metric("Latest Ingestion", relative_time(metrics["LATEST_INGESTION"]))
    
    st.divider()
    
    # --- PART 2: Log Timeline ---
    @st.cache_data(ttl=60)
    def get_timeline_data():
        df = session.sql("""
            SELECT
                DATE_TRUNC('HOUR', TRY_TO_TIMESTAMP(TIMESTAMP)) AS HOUR_BUCKET,
                CASE
                    WHEN LOG_LEVEL IN ('FATAL','CRIT') THEN 'FATAL/CRIT'
                    WHEN LOG_LEVEL = 'ERROR' THEN 'ERROR'
                    WHEN LOG_LEVEL IN ('WARN','WARNING') THEN 'WARN'
                    ELSE 'INFO'
                END AS LEVEL_GROUP,
                COUNT(*) AS LOG_COUNT
            FROM KAFKA_LOGS.RAW.PARSED_LOGS
            WHERE TRY_TO_TIMESTAMP(TIMESTAMP) IS NOT NULL
            GROUP BY HOUR_BUCKET, LEVEL_GROUP
            ORDER BY HOUR_BUCKET
        """).to_pandas()
        return df
    
    timeline_df = get_timeline_data()
    
    if not timeline_df.empty:
        import altair as alt
    
        timeline_df["HOUR_BUCKET"] = pd.to_datetime(timeline_df["HOUR_BUCKET"])
    
        color_scale = alt.Scale(
            domain=["FATAL/CRIT", "ERROR", "WARN", "INFO"],
            range=["#8B0000", "#DC3545", "#FD7E14", "#6C757D"]
        )
    
        chart = alt.Chart(timeline_df).mark_area(opacity=0.7, interpolate="monotone").encode(
            x=alt.X("HOUR_BUCKET:T", title="Time", axis=alt.Axis(format="%b %d %H:%M")),
            y=alt.Y("LOG_COUNT:Q", title="Log Count", stack=True),
            color=alt.Color("LEVEL_GROUP:N", title="Level", scale=color_scale,
                            sort=["FATAL/CRIT", "ERROR", "WARN", "INFO"]),
            order=alt.Order("LEVEL_GROUP:N", sort="descending"),
            tooltip=["HOUR_BUCKET:T", "LEVEL_GROUP:N", "LOG_COUNT:Q"]
        ).properties(height=250, title="Log Volume Timeline")
    
        st.altair_chart(chart, use_container_width=True)
    
    # --- Filter by App ---
    @st.cache_data(ttl=60)
    def get_app_names():
        df = session.sql(
            "SELECT DISTINCT APP_NAME FROM KAFKA_LOGS.RAW.PARSED_LOGS ORDER BY APP_NAME"
        ).to_pandas()
        return ["All Apps"] + df["APP_NAME"].tolist()
    
    app_filter = st.selectbox("**Filter by App**", get_app_names())
    
    # --- Filter by time range ---
    st.caption("Filter by time range:")
    tr_col1, tr_col2, tr_col3 = st.columns([2, 2, 1])
    time_options = [""] + [f"{h:02d}:{m:02d}" for h in range(24) for m in (0, 15, 30, 45)]
    with tr_col1:
        start_date = st.date_input("Start Date", value=None, key="start_date")
        start_time = st.selectbox("Start Time", options=time_options, index=0, key="start_time", format_func=lambda x: "HH:mm" if x == "" else x)
    with tr_col2:
        end_date = st.date_input("End Date", value=None, key="end_date")
        end_time = st.selectbox("End Time", options=time_options, index=0, key="end_time", format_func=lambda x: "HH:mm" if x == "" else x)
    with tr_col3:
        st.write("")
        st.write("")
        time_filter_active = st.button("Apply Filter")
    
    # Build time filter SQL clause
    time_filter_clause = ""
    if start_date and start_time:
        start_dt = f"{start_date} {start_time}"
        time_filter_clause += f" AND INGESTED_AT >= '{start_dt}'"
    if end_date and end_time:
        end_dt = f"{end_date} {end_time}"
        time_filter_clause += f" AND INGESTED_AT <= '{end_dt}'"
    
    st.divider()
    
    search_query = st.text_input(
        "**Search logs keyword**",
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
    
    def get_service_context(component):
        if not component:
            return None
        escaped = component.replace("'", "''")
        df = session.sql(f"""
            SELECT TIER, TEAM_OWNER, BUSINESS_FUNCTION, AFFECTED_CUSTOMERS,
                   LAST_DEPLOY_VERSION, LAST_DEPLOY_DATE
            FROM KAFKA_LOGS.RAW.SERVICE_REGISTRY
            WHERE UPPER(SERVICE_NAME) = UPPER('{escaped}')
               OR UPPER('{escaped}') LIKE '%' || UPPER(SERVICE_NAME) || '%'
               OR UPPER(SERVICE_NAME) LIKE '%' || UPPER('{escaped}') || '%'
            LIMIT 1
        """).to_pandas()
        if df.empty:
            return None
        return df.iloc[0].to_dict()
    
    def call_ai_complete(system_prompt, user_content):
        models = ["llama3.3-70b", "llama3.1-70b"]
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
            query_words = [w.lower() for w in re.findall(r'\w{3,}', search_query)]
            if query_words:
                all_messages = " ".join(
                    r.get("MESSAGE", "") or "" for r in results
                ).lower()
                matched_words = [w for w in query_words if w in all_messages]
                if not matched_words:
                    results = []
    
        if not results:
            st.info("No relevant logs found for your query. Try different keywords or check your APP_NAME filter.")
        elif results:
            logs_df = pd.DataFrame(results)
            logs_df = logs_df.drop(columns=["@scores"], errors="ignore")
            logs_df.columns = [c.upper() for c in logs_df.columns]
    
            # Apply time filter if active
            if time_filter_clause:
                components_filter = logs_df["COMPONENT"].unique().tolist() if "COMPONENT" in logs_df.columns else []
                if components_filter and "MESSAGE" in logs_df.columns:
                    time_filtered_df = session.sql(f"""
                        SELECT APP_NAME, LOG_LEVEL, COMPONENT, MESSAGE
                        FROM KAFKA_LOGS.RAW.PARSED_LOGS
                        WHERE MESSAGE IN (SELECT MESSAGE FROM KAFKA_LOGS.RAW.PARSED_LOGS WHERE 1=1 {time_filter_clause})
                        {time_filter_clause}
                        LIMIT 50
                    """).to_pandas()
                    if not time_filtered_df.empty:
                        logs_df = time_filtered_df
                        logs_df.columns = [c.upper() for c in logs_df.columns]
    
            # --- Business Impact Panel ---
            components_in_results = logs_df["COMPONENT"].unique().tolist() if "COMPONENT" in logs_df.columns else []
            service_contexts = {}
            for comp in components_in_results:
                ctx = get_service_context(comp)
                if ctx:
                    service_contexts[comp] = ctx
    
            if service_contexts:
                st.subheader("Business Impact")
                for comp, ctx in service_contexts.items():
                    tier = ctx.get("TIER", "Unknown")
                    if tier == "Tier-1":
                        tier_color = "red"
                    elif tier == "Tier-2":
                        tier_color = "orange"
                    else:
                        tier_color = "green"
    
                    col1, col2, col3, col4, col5 = st.columns(5)
                    with col1:
                        st.markdown(f"**Service:** {comp}")
                        st.markdown(f":{tier_color}[**{tier}**]")
                    with col2:
                        st.markdown(f"**Business Function:**")
                        st.markdown(f"{ctx.get('BUSINESS_FUNCTION', 'N/A')}")
                    with col3:
                        st.markdown(f"**Customer Impact:**")
                        st.markdown(f"{ctx.get('AFFECTED_CUSTOMERS', 'N/A')}")
                    with col4:
                        st.markdown(f"**Team Owner:**")
                        st.markdown(f"{ctx.get('TEAM_OWNER', 'N/A')}")
                    with col5:
                        deploy_ver = ctx.get("LAST_DEPLOY_VERSION", "N/A")
                        deploy_date = ctx.get("LAST_DEPLOY_DATE", "N/A")
                        if deploy_date and deploy_date != "N/A":
                            deploy_date = str(deploy_date)[:10]
                        st.markdown(f"**Last Deploy:**")
                        st.markdown(f"{deploy_ver} ({deploy_date})")
                    st.divider()
    
            st.subheader(f"Matching Log Entries ({len(logs_df)} results)")
            logs_df["SEVERITY"] = logs_df["MESSAGE"].apply(classify_severity)
    
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
    
            # Diagnose button
            if st.button("Diagnose", type="primary"):
                import time as _time
                trace_steps = []
                trace_start = _time.time()
\N
                with st.status("Agent Execution Trace", expanded=True) as status:
                    # Step 1: Collect logs
                    st.write("**Step 1: Collect matching logs**")
                    log_lines = "\n".join(
                        f"[{row.get('LOG_LEVEL', 'N/A')}] [{row.get('COMPONENT', '')}] {row.get('MESSAGE', '')}"
                        for _, row in logs_df.iterrows()
                    )
                    step1_time = _time.time() - trace_start
                    st.caption(f"{len(logs_df)} entries found for query '{search_query}' — {step1_time:.2f}s")
                    trace_steps.append({"step": "Collect matching logs", "detail": f"{len(logs_df)} entries found for query '{search_query}'", "ts": step1_time})
\N
                    # Step 2: Service Registry lookup
                    st.write("**Step 2: Service Registry lookup**")
                    service_context_str = ""
                    if service_contexts:
                        for comp, ctx in service_contexts.items():
                            tier = ctx.get("TIER", "Unknown")
                            bfunc = ctx.get("BUSINESS_FUNCTION", "Unknown")
                            affected = ctx.get("AFFECTED_CUSTOMERS", "Unknown")
                            deploy_ver = ctx.get("LAST_DEPLOY_VERSION", "Unknown")
                            deploy_date = str(ctx.get("LAST_DEPLOY_DATE", "Unknown"))[:10]
                            service_context_str += (
                                f"This is a {tier} service handling {bfunc}, affecting {affected}. "
                                f"Last deployment was {deploy_ver} on {deploy_date}. "
                                f"Consider whether the error could be related to the recent deployment. "
                                f"Assess business impact in your diagnosis.\n"
                            )
                    step2_time = _time.time() - trace_start
                    svc_detail = f"{len(service_contexts)} service(s) matched" if service_contexts else "No service match"
                    st.caption(f"{svc_detail} — {step2_time:.2f}s")
                    trace_steps.append({"step": "Service Registry lookup", "detail": svc_detail, "ts": step2_time})
\N
                    # Step 3: Build AI prompt
                    st.write("**Step 3: Build AI prompt**")
                    analysis_prompt = (
                        "You are an infrastructure and DevOps expert. "
                        + (service_context_str if service_context_str else "")
                        + "Analyze these log entries and return your analysis in EXACTLY this format:\n\n"
                        "ROOT CAUSE: <one clear sentence>\n"
                        "SEVERITY: <critical/high/medium/low>\n"
                        "IMPACT: <what this affects>\n"
                        "FIX STEPS:\n1. <step>\n2. <step>\n3. <step>\n"
                        "PREVENTION: <how to prevent>\n\n"
                        "Be specific and actionable."
                    )
                    user_content = f"User's search: {search_query}\n\nMatching log entries:\n{log_lines}"
                    step3_time = _time.time() - trace_start
                    st.caption(f"Model: llama3.3-70b | Context: {len(user_content)} chars — {step3_time:.2f}s")
                    trace_steps.append({"step": "Build AI prompt", "detail": f"Model: llama3.3-70b | Context: {len(user_content)} chars", "ts": step3_time})
\N
                    # Step 4: Call AI_COMPLETE
                    st.write("**Step 4: AI_COMPLETE diagnosis**")
                    response = call_ai_complete(analysis_prompt, user_content)
                    step4_time = _time.time() - trace_start
                    st.caption(f"Response: {len(response) if response else 0} chars — {step4_time:.2f}s")
                    trace_steps.append({"step": "AI_COMPLETE diagnosis", "detail": f"Response: {len(response) if response else 0} chars", "ts": step4_time})
\N
                    # Step 5: Finalize
                    st.write("**Step 5: Finalize report**")
                    step5_time = _time.time() - trace_start
                    st.caption(f"Diagnosis stored in session — {step5_time:.2f}s")
                    trace_steps.append({"step": "Finalize report", "detail": "Diagnosis stored in session", "ts": step5_time})
\N
                    status.update(label=f"Agent Execution Trace — completed in {step5_time:.2f}s", state="complete", expanded=False)
\N
                st.session_state["diagnosis_response"] = response
                st.session_state["diagnosis_components"] = components_in_results
                st.session_state["diagnosis_service_contexts"] = service_contexts
                st.session_state["diagnosis_search_query"] = search_query
                st.session_state["diagnosis_app_filter"] = app_filter
                st.session_state["diagnosis_log_count"] = len(logs_df)
                st.session_state["show_past_fixes"] = False
                st.session_state["show_resolution_form"] = False
                st.session_state["agent_trace"] = trace_steps
    
            # Display diagnosis from session state
            if "diagnosis_response" in st.session_state:
                response = st.session_state.get("diagnosis_response")
                st.divider()
                st.subheader("Diagnosis Report")
    
                if response:
                    cleaned = response.replace("\n", "\n")
                    for line in cleaned.strip().split("\n"):
                        line = line.strip()
                        if line:
                            st.write(line)
                else:
                    st.caption("(AI unavailable on trial account - showing pattern-based analysis)")
    
                    error_count = len(logs_df[logs_df.get("LOG_LEVEL", pd.Series()) == "ERROR"]) if "LOG_LEVEL" in logs_df.columns else 0
                    warn_count = len(logs_df[logs_df.get("LOG_LEVEL", pd.Series()) == "WARN"]) if "LOG_LEVEL" in logs_df.columns else 0
                    components = logs_df["COMPONENT"].unique().tolist() if "COMPONENT" in logs_df.columns else []
    
                    if error_count > 5:
                        sev = "CRITICAL"
                    elif error_count > 0:
                        sev = "HIGH"
                    elif warn_count > 5:
                        sev = "MEDIUM"
                    else:
                        sev = "LOW"
    
                    report_data = []
                    report_data.append({"Field": "Search Query", "Details": search_query})
                    report_data.append({"Field": "Matching Logs", "Details": f"{len(logs_df)} entries found"})
                    report_data.append({"Field": "Severity", "Details": sev})
                    report_data.append({"Field": "Error Count", "Details": str(error_count)})
                    report_data.append({"Field": "Warning Count", "Details": str(warn_count)})
                    report_data.append({"Field": "Components", "Details": ", ".join(components[:5])})
    
                    if "MESSAGE" in logs_df.columns:
                        messages = logs_df["MESSAGE"].tolist()
                        patterns = []
                        for msg in messages[:10]:
                            short = msg[:100] if msg else ""
                            if short and short not in patterns:
                                patterns.append(short)
    
                        for i, p in enumerate(patterns[:3]):
                            report_data.append({"Field": f"Pattern {i+1}", "Details": p})
    
                    report_df = pd.DataFrame(report_data)
                    st.table(report_df)
    
                # --- ACTIONS SECTION ---
                st.divider()
                st.markdown("### Take Action")
    
                diagnosis_text = response if response else ""
                primary_component = components_in_results[0] if components_in_results else "Unknown"
                primary_app = app_filter if app_filter != "All Apps" else (logs_df["APP_NAME"].iloc[0] if "APP_NAME" in logs_df.columns and len(logs_df) > 0 else "Unknown")
    
                diag_lower = diagnosis_text.lower() if diagnosis_text else ""
                if "critical" in diag_lower:
                    diag_severity = "Critical"
                    diag_priority = "P1"
                elif "high" in diag_lower:
                    diag_severity = "High"
                    diag_priority = "P2"
                elif "medium" in diag_lower:
                    diag_severity = "Medium"
                    diag_priority = "P3"
                else:
                    diag_severity = "Low"
                    diag_priority = "P4"
    
                top_col1, top_col2 = st.columns([1, 1])
    
                with top_col1:
                    if st.button("🚨 Create Incident Ticket", key="btn_create_ticket"):
                        title_summary = diagnosis_text[:50] if diagnosis_text else search_query[:50]
                        ticket_title = f"[{diag_severity}] [{primary_component}] - {title_summary}"
                        escaped_title = ticket_title.replace("'", "''")
                        escaped_desc = diagnosis_text.replace("'", "''") if diagnosis_text else ""
                        escaped_comp = primary_component.replace("'", "''")
                        escaped_appn = primary_app.replace("'", "''")
                        escaped_sq = search_query.replace("'", "''")
                        session.sql(f"""
                            INSERT INTO KAFKA_LOGS.RAW.INCIDENT_TICKETS (TITLE, DESCRIPTION, COMPONENT, APP_NAME, SEVERITY, PRIORITY, SEARCH_QUERY)
                            VALUES ('{escaped_title}', '{escaped_desc}', '{escaped_comp}', '{escaped_appn}', '{diag_severity}', '{diag_priority}', '{escaped_sq}')
                        """).collect()
                        ticket_id_df = session.sql("SELECT MAX(TICKET_ID) AS TID FROM KAFKA_LOGS.RAW.INCIDENT_TICKETS").to_pandas()
                        tid = ticket_id_df["TID"].iloc[0]
                        st.success(f"Incident ticket #{tid} created")
                        st.cache_data.clear()
                    st.caption("Auto-generate a P1-P4 incident with diagnosis details")
    
                with top_col2:
                    if st.button("🔍 Search Past Fixes", key="btn_past_fixes"):
                        st.session_state["show_past_fixes"] = not st.session_state.get("show_past_fixes", False)
                    st.caption("Check if this issue was resolved before")
    
                st.markdown("<br>", unsafe_allow_html=True)
    
                bot_col1, bot_col2 = st.columns([1, 1])
    
                with bot_col1:
                    if st.button("✅ Log Resolution", key="btn_log_resolution"):
                        st.session_state["show_resolution_form"] = True
                    st.caption("Record what you did to fix this issue")
    
                with bot_col2:
                    report_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    report_filename = f"logsense_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
                    business_impact_str = ""
                    if service_contexts:
                        for comp, ctx in service_contexts.items():
                            business_impact_str += f"  Service: {comp}, Tier: {ctx.get('TIER','N/A')}, Function: {ctx.get('BUSINESS_FUNCTION','N/A')}, Customers: {ctx.get('AFFECTED_CUSTOMERS','N/A')}\n"
                    report_content = f"""LogSense AI - Diagnosis Report
    ================================
    Report Generated: {report_timestamp}
    Search Query: {search_query}
    App Name: {primary_app}
    Matched Logs: {len(logs_df)}
    
    Business Impact:
    {business_impact_str if business_impact_str else '  No service registry data available'}
    
    AI Diagnosis:
    {diagnosis_text if diagnosis_text else 'N/A'}
    
    Recommended Actions:
    - Review the root cause identified above
    - Follow the fix steps provided
    - Implement prevention measures
    ================================
    """
                    st.download_button("📄 Export Report", data=report_content, file_name=report_filename, mime="text/plain", key="btn_export")
                    st.caption("Download a full diagnostic report")
    
                # Resolution form
                if st.session_state.get("show_resolution_form"):
                    resolution_notes = st.text_area("What did you do to fix this issue?", key="resolution_notes_input")
                    if st.button("Save Resolution", key="btn_save_resolution"):
                        if resolution_notes:
                            escaped_notes = resolution_notes.replace("'", "''")
                            escaped_sq = search_query.replace("'", "''")
                            escaped_diag = (diagnosis_text[:500] if diagnosis_text else "").replace("'", "''")
                            escaped_comp = primary_component.replace("'", "''")
                            escaped_appn = primary_app.replace("'", "''")
                            session.sql(f"""
                                INSERT INTO KAFKA_LOGS.RAW.AUDIT_RESOLUTIONS (SEARCH_QUERY, DIAGNOSIS_SUMMARY, RESOLUTION_NOTES, COMPONENT, APP_NAME)
                                VALUES ('{escaped_sq}', '{escaped_diag}', '{escaped_notes}', '{escaped_comp}', '{escaped_appn}')
                            """).collect()
                            st.success("Resolution logged successfully")
                            st.session_state["show_resolution_form"] = False
                        else:
                            st.warning("Please enter resolution notes.")
    
                # Past fixes search
                if st.session_state.get("show_past_fixes"):
                    past_fixes_df = session.sql("""
                        SELECT RESOLUTION_NOTES, DIAGNOSIS_SUMMARY, COMPONENT, APP_NAME, RESOLVED_AT
                        FROM KAFKA_LOGS.RAW.AUDIT_RESOLUTIONS
                        WHERE RESOLUTION_NOTES IS NOT NULL
                        ORDER BY RESOLVED_AT DESC
                        LIMIT 10
                    """).to_pandas()
    
                    if past_fixes_df.empty:
                        st.info("No past resolutions found for this pattern. This may be a new issue.")
                    else:
                        past_notes = "\n".join(
                            f"- [{row['COMPONENT']}] {row['RESOLUTION_NOTES']}" 
                            for _, row in past_fixes_df.iterrows() if row['RESOLUTION_NOTES']
                        )
                        compare_prompt = "You are a helpful assistant. Compare the current diagnosis with past resolutions and identify the most relevant match. If any past resolution is related, summarize it. Only say 'No relevant match found' if none of the past resolutions are even remotely related."
                        compare_content = f"Current diagnosis:\n{diagnosis_text}\n\nPast resolutions:\n{past_notes}"
                        match_result = call_ai_complete(compare_prompt, compare_content)
                        if match_result and "no relevant match" not in match_result.lower():
                            most_recent = str(past_fixes_df["RESOLVED_AT"].iloc[0])[:16]
                            cleaned_match = match_result.replace("\n", "\n")
                            st.info(f"Similar issue resolved on {most_recent}:")
                            for line in cleaned_match.strip().split("\n"):
                                line = line.strip()
                                if line:
                                    st.markdown(line)
                        else:
                            st.info("No past resolutions found for this pattern. This may be a new issue.")
    
