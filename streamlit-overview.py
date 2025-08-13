import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, max as sf_max, min as sf_min, avg, count, array_agg, dateadd, current_date, lit, datediff
from snowflake.snowpark.types import IntegerType
from datetime import datetime, timedelta

# Get the current Snowflake session
session = get_active_session()

st.set_page_config(layout="wide")

# Set up the page
st.title("Disney Corporate Overview")
st.markdown("---")

# Add a loading message
with st.spinner('Loading data from Snowflake...'):
    try:
        # Get the base table
        df = session.table("TEMP.AMENDELSON.CORPORATE_RR")
        
        # Get the maximum date in the dataset
        max_date_result = df.select(sf_max(col("USAGE_DATE")).alias("MAX_DATE")).collect()
        max_date = max_date_result[0]["MAX_DATE"]

        # Calculate overall spend for the account 
        df_overall_rr = session.sql("""SELECT
                                        usage_date,
                                        SUM(total_credits) AS total_daily_credits
                                        FROM TEMP.AMENDELSON.CORPORATE_RR
                                        WHERE usage_date >= DATE('2024-01-01')
                                        GROUP BY 1
                                        ORDER BY usage_date DESC""")
         # Create overall RR line chart
        st.subheader("Total Daily Credits Over Time")
        
        # Collect the data for the chart
        overall_rr_data = df_overall_rr.collect()
        
        # Create the line chart
        fig_overall = go.Figure(data=[
            go.Scatter(
                x=[row["USAGE_DATE"] for row in overall_rr_data],
                y=[row["TOTAL_DAILY_CREDITS"] for row in overall_rr_data],
                mode='lines+markers',
                name='Total Daily Credits',
                line=dict(width=2, color='steelblue'),
                marker=dict(size=4),
                hovertemplate='<b>Total Daily Credits</b><br>Date: %{x}<br>Credits: %{y:,.0f}<extra></extra>'
            )
        ])
        
        fig_overall.update_layout(
            xaxis_title="Date",
            yaxis_title="Total Daily Credits",
            height=600,
            showlegend=False,
            yaxis=dict(tickformat=',.0f',
            range=[0, None],
            rangemode='tozero'),  
            xaxis=dict(tickangle=45)
        )
        
        # Display the chart
        st.plotly_chart(fig_overall, use_container_width=True)
        st.markdown("---")
        
        # Filter for rolling 30-day period using Snowpark
        # Use datediff to filter for last 30 days
        df_filtered = session.sql("""SELECT 
                                    snowflake_group_rollup,
                                    SUM(total_credits) AS total_credits,
                                    SUM(annual_rr_credits) AS annualized_rr_credits,
                                    SUM(annual_rr_dollars) AS annualized_rr_dollars
                                    FROM corporate_rr
                                    WHERE usage_date >= CURRENT_DATE()-30
                                    AND usage_date < CURRENT_DATE()
                                    GROUP BY 1
                                    ORDER BY 2 DESC""")
        df_charts = session.sql("""SELECT 
                                    snowflake_group_rollup,
                                    SUM(total_credits) AS total_credits,
                                    SUM(annual_rr_credits) AS annualized_rr_credits,
                                    SUM(annual_rr_dollars) AS annualized_rr_dollars
                                    FROM corporate_rr
                                    WHERE usage_date >= CURRENT_DATE()-365
                                    AND usage_date < CURRENT_DATE()
                                    GROUP BY 1
                                    ORDER BY 2 DESC""")
        
       
        # Get top 10 groups by latest revenue
        top10_data = session.sql("""SELECT 
                                    snowflake_group_rollup,
                                    SUM(total_credits) AS total_credits,
                                    SUM(annual_rr_credits) AS annualized_rr_credits,
                                    SUM(annual_rr_dollars) AS annualized_rr_dollars
                                    FROM corporate_rr
                                    WHERE usage_date >= CURRENT_DATE()-30
                                    AND usage_date < CURRENT_DATE()
                                    GROUP BY 1
                                    ORDER BY 2 DESC
                                    LIMIT 10""")
        
        # Calculate summary metrics
        total_groups = df_filtered.count()
        total_revenue_df = session.sql("""SELECT 
                                    SUM(annual_rr_dollars) AS annualized_rr_dollars
                                    FROM corporate_rr
                                    WHERE usage_date >= CURRENT_DATE()-30
                                    AND usage_date < CURRENT_DATE()
                                    """)
        
        total_revenue = total_revenue_df.collect()[0]['ANNUALIZED_RR_DOLLARS']
            
        # Display summary metrics
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric(
                label="Total Groups",
                value=total_groups
            )
        
        with col2:
            st.metric(
                label="Annualized RR",
                value=f"${total_revenue:,.0f}"
            )
        
        # Format dates for display
        cutoff_date = max_date - timedelta(days=30)
        st.info(f"Data filtered for period: {cutoff_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}")
        
        st.markdown("---")
        
        # Create the horizontal bar chart for current top 10
        st.subheader("Top 10 Annualized Consumption by Snowflake Group ")
        st.write("RR = 30 day consumption * 12")
        
        # Prepare data for chart (reverse order for horizontal bar)
        chart_data = list(reversed(top10_data.collect()))
        
        # Create Plotly horizontal bar chart
        fig = go.Figure(data=[
            go.Bar(
                x=[row["ANNUALIZED_RR_DOLLARS"] for row in chart_data],
                y=[row["SNOWFLAKE_GROUP_ROLLUP"] for row in chart_data],
                orientation='h',
                marker_color='steelblue',
                text=[row["ANNUALIZED_RR_DOLLARS"] for row in chart_data],
                texttemplate='$%{text:,.0f}',
                textposition='outside',
                hovertemplate='<b>%{y}</b><br>Revenue: $%{x:,.0f}<extra></extra>'
            )
        ])
        
        fig.update_layout(
            xaxis_title="Annual RR Dollars",
            yaxis_title="Snowflake Group Rollup",
            height=600,
            showlegend=False,
            xaxis=dict(tickformat='$,.0f'),
            margin=dict(l=200)  # Add left margin for group names
        )
        
        # Display the chart
        st.plotly_chart(fig, use_container_width=True)
        st.markdown("---")
        
        # Create line chart small multiples for each group
        st.subheader("📈 Time Series - Annual RR by Group (Full Year Data)")
        
        # Get top 10 groups by credit consumption from the 30-day data
        top10_data_collected = top10_data.collect()
        top10_groups = [row["SNOWFLAKE_GROUP_ROLLUP"] for row in top10_data_collected]
        
        # Get time series data for top groups using full year data
        time_series_data = {}
        for group in top10_groups:
            if group:  # Only process non-None groups
                # Escape single quotes for SQL safety
                escaped_group = str(group).replace("'", "''")
                
                # Get daily time series data for this specific group over the full year
                group_data = session.sql(f"""
                    SELECT 
                        usage_date,
                        annual_rr_dollars
                    FROM TEMP.AMENDELSON.CORPORATE_RR
                    WHERE snowflake_group_rollup = '{escaped_group}'
                    AND usage_date >= CURRENT_DATE()-365
                    AND usage_date < CURRENT_DATE()
                    ORDER BY usage_date
                """).collect()
                
                if group_data:  # Only add if we have data
                    time_series_data[group] = {
                        'dates': [row["USAGE_DATE"] for row in group_data],
                        'values': [row["ANNUAL_RR_DOLLARS"] for row in group_data]
                    }
        
        # Calculate subplot dimensions
        n_groups = len([g for g in top10_groups if g in time_series_data])
        
        if n_groups > 0:
            cols = 2  # 2 columns for better readability
            rows = (n_groups + cols - 1) // cols  # Calculate rows needed
            
            # Create subplots
            fig_lines = make_subplots(
                rows=rows, 
                cols=cols,
                subplot_titles=[g for g in top10_groups if g in time_series_data],
                vertical_spacing=0.12,
                horizontal_spacing=0.1
            )
            
            # Add line chart for each group
            plot_index = 0
            for group in top10_groups:
                if group in time_series_data and len(time_series_data[group]['dates']) > 0:
                    row = plot_index // cols + 1
                    col = plot_index % cols + 1
                    
                    fig_lines.add_trace(
                        go.Scatter(
                            x=time_series_data[group]['dates'],
                            y=time_series_data[group]['values'],
                            mode='lines+markers',
                            name=group,
                            showlegend=False,
                            line=dict(width=2, color='steelblue'),
                            marker=dict(size=3),
                            hovertemplate='<b>' + group + '</b><br>Date: %{x}<br>Revenue: $%{y:,.0f}<extra></extra>'
                        ),
                        row=row, col=col
                    )
                    plot_index += 1
            
            # Update layout for small multiples
            fig_lines.update_layout(
                height=400 * rows,
                title_text="",
                showlegend=False,
                font=dict(size=10)
            )
            
            # Update all y-axes to show currency format
            fig_lines.update_yaxes(tickformat='$,.0s')
            fig_lines.update_xaxes(tickangle=45)
            
            st.plotly_chart(fig_lines, use_container_width=True)
        else:
            st.warning("No time series data available for the selected groups.")
        
        st.markdown("---")
        
       
        
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        st.write("Please check:")
        st.write("- Table name: TEMP.AMENDELSON.CORPORATE_RR")
        st.write("- Column names: SNOWFLAKE_GROUP_ROLLUP, ANNUAL_RR_DOLLARS, USAGE_DATE")
        st.write("- Database permissions")
        
        # Debug information
        if st.checkbox("Show debug information"):
            st.write("Error details:", str(e))

# Add footer
st.markdown("---")
st.markdown("*Data source: TEMP.AMENDELSON.CORPORATE_RR (30-day rolling window)*")
