import os
import sys
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from dash import Dash, html, dcc, dash_table
from dash.dependencies import Input, Output
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

try:
    from pymongo import MongoClient
    MONGODB_AVAILABLE = True
except ImportError:
    MONGODB_AVAILABLE = False

from config.app_config import Config

def get_db():
    client = MongoClient(Config.MONGODB_URI)
    return client[Config.MONGODB_DATABASE]

def fetch_alerts(limit=50):
    try:
        db = get_db()
        alerts = list(db["fraud_alerts"].find(
            {"is_fraud_predicted": True}
        ).sort("processing_time", -1).limit(limit))
        
        if not alerts:
            return pd.DataFrame({
                "transaction_id": ["sample-1", "sample-2"],
                "user_id": ["USER_001", "USER_002"],
                "amount": [100.0, 500.0],
                "country": ["US", "NG"],
                "fraud_score": [0.75, 0.92]
            })
        
        df = pd.DataFrame(alerts)
        if "_id" in df.columns:
            df = df.drop("_id", axis=1)
        return df
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()

def fetch_stats():
    try:
        db = get_db()
        total = db["realtime_views"].count_documents({})
        fraud = db["fraud_alerts"].count_documents({})
        return {
            "total": total,
            "fraud": fraud,
            "rate": fraud / max(1, total)
        }
    except:
        return {"total": 0, "fraud": 0, "rate": 0}

def fetch_by_country():
    try:
        db = get_db()
        pipeline = [
            {"$group": {
                "_id": "$country",
                "count": {"$sum": 1},
                "fraud": {"$sum": {"$cond": ["$is_fraud_predicted", 1, 0]}}
            }},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]
        results = list(db["realtime_views"].aggregate(pipeline))
        if results:
            return pd.DataFrame(results).rename(columns={"_id": "country"})
        return pd.DataFrame({"country": ["US", "NG", "UK"], "count": [10, 5, 3], "fraud": [1, 3, 0]})
    except:
        return pd.DataFrame({"country": ["US", "NG", "UK"], "count": [10, 5, 3], "fraud": [1, 3, 0]})

app = Dash(__name__)

app.layout = html.Div([
    html.H1(" Fraud Detection Dashboard", 
            style={"textAlign": "center", "color": "#2c3e50", "marginTop": "20px"}),
    html.P("Lambda Architecture - Real-Time Fraud Detection",
           style={"textAlign": "center", "color": "#7f8c8d"}),
    
    dcc.Interval(id="refresh", interval=10000, n_intervals=0),
    
    html.Div([
        html.Div([
            html.H2(id="stat-total", style={"color": "#3498db"}),
            html.P("Total Transactions")
        ], style={"flex": 1, "textAlign": "center", "padding": "20px", 
                  "backgroundColor": "#ecf0f1", "margin": "10px", "borderRadius": "10px"}),
        
        html.Div([
            html.H2(id="stat-fraud", style={"color": "#e74c3c"}),
            html.P("Fraud Detected")
        ], style={"flex": 1, "textAlign": "center", "padding": "20px",
                  "backgroundColor": "#ecf0f1", "margin": "10px", "borderRadius": "10px"}),
        
        html.Div([
            html.H2(id="stat-rate", style={"color": "#f39c12"}),
            html.P("Fraud Rate")
        ], style={"flex": 1, "textAlign": "center", "padding": "20px",
                  "backgroundColor": "#ecf0f1", "margin": "10px", "borderRadius": "10px"}),
    ], style={"display": "flex", "justifyContent": "center", "marginBottom": "20px"}),
    
    html.Div([
        html.Div([
            html.H3(" Fraud by Country"),
            dcc.Graph(id="country-chart")
        ], style={"flex": 1, "padding": "20px"}),
    ], style={"display": "flex"}),
    
    html.Div([
        html.H3(" Recent Fraud Alerts"),
        html.Div(id="alerts-table")
    ], style={"padding": "20px"}),
    
    html.P(id="last-update", style={"textAlign": "center", "color": "#95a5a6"})
    
], style={"fontFamily": "Arial, sans-serif", "maxWidth": "1200px", "margin": "0 auto"})

@app.callback(
    [Output("stat-total", "children"),
     Output("stat-fraud", "children"),
     Output("stat-rate", "children")],
    [Input("refresh", "n_intervals")]
)
def update_stats(n):
    stats = fetch_stats()
    return (
        f"{stats['total']:,}",
        f"{stats['fraud']:,}",
        f"{stats['rate']:.1%}"
    )

@app.callback(
    Output("country-chart", "figure"),
    [Input("refresh", "n_intervals")]
)
def update_country_chart(n):
    df = fetch_by_country()
    fig = px.bar(df, x="country", y="count", color="fraud",
                 title="", color_continuous_scale="Reds")
    fig.update_layout(
        plot_bgcolor="white",
        paper_bgcolor="white",
        showlegend=False,
        height=300
    )
    return fig

@app.callback(
    Output("alerts-table", "children"),
    [Input("refresh", "n_intervals")]
)
def update_table(n):
    df = fetch_alerts(20)
    if df.empty:
        return html.P("No alerts yet...")
    
    cols = ["transaction_id", "user_id", "amount", "country", "fraud_score"]
    cols = [c for c in cols if c in df.columns]
    
    return dash_table.DataTable(
        data=df[cols].head(10).to_dict("records"),
        columns=[{"name": c.replace("_", " ").title(), "id": c} for c in cols],
        style_table={"overflowX": "auto"},
        style_cell={"textAlign": "left", "padding": "10px"},
        style_header={"backgroundColor": "#3498db", "color": "white", "fontWeight": "bold"},
        style_data_conditional=[
            {"if": {"row_index": "odd"}, "backgroundColor": "#f8f9fa"}
        ]
    )

@app.callback(
    Output("last-update", "children"),
    [Input("refresh", "n_intervals")]
)
def update_timestamp(n):
    return f"Last updated: {datetime.now().strftime('%H:%M:%S')} | Auto-refresh every 10s"

if __name__ == "__main__":
    print("=" * 50)
    print("SIMPLE FRAUD DETECTION DASHBOARD")
    print("=" * 50)
    print(f"MongoDB: {Config.MONGODB_URI}")
    print("=" * 50)
    print("\n Dashboard: http://localhost:8050\n")
    
    app.run_server(debug=False, host="0.0.0.0", port=8050)
