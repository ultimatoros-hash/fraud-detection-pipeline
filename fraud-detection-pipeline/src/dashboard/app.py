import os
import sys
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    import dash
    from dash import dcc, html, dash_table
    from dash.dependencies import Input, Output
    import dash_bootstrap_components as dbc
    import plotly.express as px
    import plotly.graph_objects as go
    import pandas as pd
    DASH_AVAILABLE = True
except ImportError:
    DASH_AVAILABLE = False
    print(" Dash not installed. Run: pip install dash dash-bootstrap-components plotly")

try:
    from pymongo import MongoClient
    MONGODB_AVAILABLE = True
except ImportError:
    MONGODB_AVAILABLE = False

from config.app_config import Config

def get_mongodb_client():
    return MongoClient(Config.MONGODB_URI)

def fetch_recent_alerts(limit: int = 50) -> pd.DataFrame:
    try:
        client = get_mongodb_client()
        db = client[Config.MONGODB_DATABASE]
        
        alerts = list(db[Config.MONGODB_COLLECTION_FRAUD_ALERTS].find(
            {"is_fraud_predicted": True}
        ).sort("processing_time", -1).limit(limit))
        
        client.close()
        
        if not alerts:
            return pd.DataFrame()
        
        df = pd.DataFrame(alerts)
        
        if "_id" in df.columns:
            df = df.drop("_id", axis=1)
        
        return df
        
    except Exception as e:
        print(f"Error fetching alerts: {e}")
        return pd.DataFrame()

def fetch_fraud_by_country() -> pd.DataFrame:
    try:
        client = get_mongodb_client()
        db = client[Config.MONGODB_DATABASE]
        
        pipeline = [
            {
                "$group": {
                    "_id": "$country",
                    "total": {"$sum": 1},
                    "fraud_count": {"$sum": {"$cond": ["$is_fraud_predicted", 1, 0]}},
                    "total_amount": {"$sum": "$amount"}
                }
            },
            {"$sort": {"total": -1}},
            {"$limit": 15}
        ]
        
        results = list(db[Config.MONGODB_COLLECTION_FRAUD_ALERTS].aggregate(pipeline))
        client.close()
        
        if not results:
            return pd.DataFrame({"country": [], "total": [], "fraud_count": []})
        
        df = pd.DataFrame(results)
        df = df.rename(columns={"_id": "country"})
        df["fraud_rate"] = df["fraud_count"] / df["total"].replace(0, 1)
        
        return df
        
    except Exception as e:
        print(f"Error fetching country data: {e}")
        return pd.DataFrame({"country": [], "total": [], "fraud_count": []})

def fetch_fraud_over_time() -> pd.DataFrame:
    try:
        client = get_mongodb_client()
        db = client[Config.MONGODB_DATABASE]
        
        stats = list(db["dashboard_stats"].find().sort("timestamp", -1).limit(100))
        client.close()
        
        if not stats:
            return pd.DataFrame({
                "timestamp": pd.date_range(end=datetime.now(), periods=24, freq="H"),
                "fraud_rate": [0.05] * 24,
                "total": [100] * 24
            })
        
        df = pd.DataFrame(stats)
        if "_id" in df.columns:
            df = df.drop("_id", axis=1)
        
        return df
        
    except Exception as e:
        print(f"Error fetching time data: {e}")
        return pd.DataFrame()

def fetch_high_risk_users(limit: int = 10) -> pd.DataFrame:
    try:
        client = get_mongodb_client()
        db = client[Config.MONGODB_DATABASE]
        
        pipeline = [
            {
                "$group": {
                    "_id": "$user_id",
                    "transactions": {"$sum": 1},
                    "fraud_count": {"$sum": {"$cond": ["$is_fraud_predicted", 1, 0]}},
                    "total_amount": {"$sum": "$amount"},
                    "avg_score": {"$avg": "$fraud_score"}
                }
            },
            {"$match": {"transactions": {"$gte": 2}}},
            {
                "$addFields": {
                    "fraud_rate": {"$divide": ["$fraud_count", "$transactions"]}
                }
            },
            {"$sort": {"fraud_rate": -1}},
            {"$limit": limit}
        ]
        
        results = list(db[Config.MONGODB_COLLECTION_FRAUD_ALERTS].aggregate(pipeline))
        client.close()
        
        if not results:
            return pd.DataFrame()
        
        df = pd.DataFrame(results)
        df = df.rename(columns={"_id": "user_id"})
        
        return df
        
    except Exception as e:
        print(f"Error fetching user data: {e}")
        return pd.DataFrame()

def fetch_summary_stats() -> dict:
    try:
        client = get_mongodb_client()
        db = client[Config.MONGODB_DATABASE]
        
        total = db[Config.MONGODB_COLLECTION_FRAUD_ALERTS].count_documents({})
        fraud = db[Config.MONGODB_COLLECTION_FRAUD_ALERTS].count_documents({"is_fraud_predicted": True})
        
        one_hour_ago = datetime.utcnow() - timedelta(hours=1)
        recent_total = db[Config.MONGODB_COLLECTION_REALTIME_VIEWS].count_documents({})
        recent_fraud = db[Config.MONGODB_COLLECTION_REALTIME_VIEWS].count_documents(
            {"is_fraud_predicted": True}
        )
        
        client.close()
        
        return {
            "total_transactions": total,
            "total_fraud": fraud,
            "fraud_rate": fraud / max(1, total),
            "recent_transactions": recent_total,
            "recent_fraud": recent_fraud,
            "recent_fraud_rate": recent_fraud / max(1, recent_total)
        }
        
    except Exception as e:
        print(f"Error fetching stats: {e}")
        return {
            "total_transactions": 0,
            "total_fraud": 0,
            "fraud_rate": 0,
            "recent_transactions": 0,
            "recent_fraud": 0,
            "recent_fraud_rate": 0
        }

if DASH_AVAILABLE:
    app = dash.Dash(
        __name__,
        external_stylesheets=[dbc.themes.DARKLY],
        title="Fraud Detection Dashboard"
    )
    
    app.layout = dbc.Container([
        dbc.Row([
            dbc.Col([
                html.H1(" Real-Time Fraud Detection Dashboard", 
                       className="text-center text-primary mb-4 mt-4"),
                html.P("Lambda Architecture - Unified View from Batch + Speed Layers",
                      className="text-center text-muted")
            ])
        ]),
        
        dcc.Interval(
            id="refresh-interval",
            interval=10 * 1000,
            n_intervals=0
        ),
        
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4(id="total-transactions", className="card-title text-info"),
                        html.P("Total Transactions", className="card-text")
                    ])
                ], color="dark", outline=True)
            ], width=3),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4(id="total-fraud", className="card-title text-danger"),
                        html.P("Fraud Detected", className="card-text")
                    ])
                ], color="dark", outline=True)
            ], width=3),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4(id="fraud-rate", className="card-title text-warning"),
                        html.P("Fraud Rate", className="card-text")
                    ])
                ], color="dark", outline=True)
            ], width=3),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4(id="recent-fraud", className="card-title text-success"),
                        html.P("Recent (1h) Fraud", className="card-text")
                    ])
                ], color="dark", outline=True)
            ], width=3),
        ], className="mb-4"),
        
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(" Fraud Rate Over Time"),
                    dbc.CardBody([
                        dcc.Graph(id="fraud-time-chart", style={"height": "300px"})
                    ])
                ], color="dark", outline=True)
            ], width=8),
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(" Transactions by Country"),
                    dbc.CardBody([
                        dcc.Graph(id="country-chart", style={"height": "300px"})
                    ])
                ], color="dark", outline=True)
            ], width=4),
        ], className="mb-4"),
        
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(" High Risk Users"),
                    dbc.CardBody([
                        dcc.Graph(id="risk-users-chart", style={"height": "300px"})
                    ])
                ], color="dark", outline=True)
            ], width=6),
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(" Fraud Score Distribution"),
                    dbc.CardBody([
                        dcc.Graph(id="score-distribution", style={"height": "300px"})
                    ])
                ], color="dark", outline=True)
            ], width=6),
        ], className="mb-4"),
        
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(" Recent Fraud Alerts (Live)"),
                    dbc.CardBody([
                        html.Div(id="alerts-table")
                    ])
                ], color="dark", outline=True)
            ])
        ], className="mb-4"),
        
        dbc.Row([
            dbc.Col([
                html.Hr(),
                html.P(
                    f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
                    "Auto-refresh every 10 seconds",
                    className="text-center text-muted",
                    id="last-update"
                )
            ])
        ])
        
    ], fluid=True)
    
    
    @app.callback(
        [
            Output("total-transactions", "children"),
            Output("total-fraud", "children"),
            Output("fraud-rate", "children"),
            Output("recent-fraud", "children"),
        ],
        [Input("refresh-interval", "n_intervals")]
    )
    def update_summary_cards(n):
        stats = fetch_summary_stats()
        return (
            f"{stats['total_transactions']:,}",
            f"{stats['total_fraud']:,}",
            f"{stats['fraud_rate']:.2%}",
            f"{stats['recent_fraud']:,}"
        )
    
    @app.callback(
        Output("fraud-time-chart", "figure"),
        [Input("refresh-interval", "n_intervals")]
    )
    def update_time_chart(n):
        df = fetch_fraud_over_time()
        
        if df.empty or "timestamp" not in df.columns:
            fig = go.Figure()
            fig.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)"
            )
            return fig
        
        fig = px.line(
            df, 
            x="timestamp", 
            y="fraud_rate" if "fraud_rate" in df.columns else "avg_fraud_score",
            title=""
        )
        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis_title="Time",
            yaxis_title="Fraud Rate",
            showlegend=False
        )
        fig.update_traces(line_color="#ff6b6b")
        
        return fig
    
    @app.callback(
        Output("country-chart", "figure"),
        [Input("refresh-interval", "n_intervals")]
    )
    def update_country_chart(n):
        df = fetch_fraud_by_country()
        
        if df.empty:
            fig = go.Figure()
            fig.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)"
            )
            return fig
        
        fig = px.bar(
            df.head(10),
            x="country",
            y="total",
            color="fraud_rate",
            color_continuous_scale="Reds",
            title=""
        )
        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis_title="Country",
            yaxis_title="Transactions",
            showlegend=False
        )
        
        return fig
    
    @app.callback(
        Output("risk-users-chart", "figure"),
        [Input("refresh-interval", "n_intervals")]
    )
    def update_risk_users_chart(n):
        df = fetch_high_risk_users()
        
        if df.empty:
            fig = go.Figure()
            fig.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)"
            )
            return fig
        
        fig = px.bar(
            df,
            x="user_id",
            y="fraud_rate",
            color="fraud_rate",
            color_continuous_scale="Reds",
            title=""
        )
        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis_title="User ID",
            yaxis_title="Fraud Rate",
            xaxis_tickangle=-45,
            showlegend=False
        )
        
        return fig
    
    @app.callback(
        Output("score-distribution", "figure"),
        [Input("refresh-interval", "n_intervals")]
    )
    def update_score_distribution(n):
        df = fetch_recent_alerts(limit=500)
        
        if df.empty or "fraud_score" not in df.columns:
            fig = go.Figure()
            fig.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)"
            )
            return fig
        
        fig = px.histogram(
            df,
            x="fraud_score",
            nbins=20,
            title=""
        )
        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis_title="Fraud Score",
            yaxis_title="Count",
            showlegend=False
        )
        fig.update_traces(marker_color="#ff6b6b")
        
        return fig
    
    @app.callback(
        Output("alerts-table", "children"),
        [Input("refresh-interval", "n_intervals")]
    )
    def update_alerts_table(n):
        df = fetch_recent_alerts(limit=20)
        
        if df.empty:
            return html.P("No fraud alerts detected yet.", className="text-muted")
        
        display_cols = ["transaction_id", "user_id", "amount", "country", 
                       "merchant_category", "fraud_score"]
        display_cols = [c for c in display_cols if c in df.columns]
        
        df_display = df[display_cols].copy()
        if "amount" in df_display.columns:
            df_display["amount"] = df_display["amount"].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "N/A")
        if "fraud_score" in df_display.columns:
            df_display["fraud_score"] = df_display["fraud_score"].apply(lambda x: f"{x:.2%}" if pd.notna(x) else "N/A")
        if "transaction_id" in df_display.columns:
            df_display["transaction_id"] = df_display["transaction_id"].apply(lambda x: x[:12] + "..." if len(str(x)) > 12 else x)
        
        table = dash_table.DataTable(
            data=df_display.to_dict("records"),
            columns=[{"name": c.replace("_", " ").title(), "id": c} for c in display_cols],
            style_table={"overflowX": "auto"},
            style_cell={
                "backgroundColor": "#303030",
                "color": "white",
                "textAlign": "left",
                "padding": "10px"
            },
            style_header={
                "backgroundColor": "#1a1a1a",
                "fontWeight": "bold",
                "color": "#17a2b8"
            },
            style_data_conditional=[
                {
                    "if": {"column_id": "fraud_score"},
                    "color": "#ff6b6b",
                    "fontWeight": "bold"
                }
            ],
            page_size=10
        )
        
        return table
    
    @app.callback(
        Output("last-update", "children"),
        [Input("refresh-interval", "n_intervals")]
    )
    def update_timestamp(n):
        return f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Auto-refresh every 10 seconds"

def main():
    if not DASH_AVAILABLE:
        print(" Dash is not installed. Please run: pip install dash dash-bootstrap-components plotly")
        return
    
    print("=" * 60)
    print("FRAUD DETECTION DASHBOARD")
    print("=" * 60)
    print(f"MongoDB: {Config.MONGODB_URI}")
    print(f"Database: {Config.MONGODB_DATABASE}")
    print("=" * 60)
    print("\n Starting dashboard at http://localhost:8050\n")
    
    app.run_server(debug=True, host="0.0.0.0", port=8050)

if __name__ == "__main__":
    main()
