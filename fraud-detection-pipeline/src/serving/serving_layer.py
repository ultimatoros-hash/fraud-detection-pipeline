import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.app_config import Config

try:
    from pymongo import MongoClient, DESCENDING
    from pymongo.collection import Collection
    MONGODB_AVAILABLE = True
except ImportError:
    MONGODB_AVAILABLE = False
    print(" pymongo not installed")

class ServingLayer:
    
    def __init__(self):
        if not MONGODB_AVAILABLE:
            raise ImportError("pymongo is required for serving layer")
        
        self.client = MongoClient(Config.MONGODB_URI)
        self.db = self.client[Config.MONGODB_DATABASE]
        
        self.batch_views: Collection = self.db[Config.MONGODB_COLLECTION_BATCH_VIEWS]
        self.realtime_views: Collection = self.db[Config.MONGODB_COLLECTION_REALTIME_VIEWS]
        self.fraud_alerts: Collection = self.db[Config.MONGODB_COLLECTION_FRAUD_ALERTS]
        self.user_profiles: Collection = self.db[Config.MONGODB_COLLECTION_USER_PROFILES]
        self.dashboard_stats: Collection = self.db["dashboard_stats"]
        self.metadata: Collection = self.db["system_metadata"]
        
        print(f" Serving Layer connected to {Config.MONGODB_URI}")
    
    def get_last_batch_timestamp(self) -> Optional[datetime]:
        doc = self.metadata.find_one({"key": "last_batch_timestamp"})
        if doc and "value" in doc:
            return doc["value"]
        return None
    
    def update_last_batch_timestamp(self, timestamp: datetime):
        self.metadata.update_one(
            {"key": "last_batch_timestamp"},
            {"$set": {"value": timestamp, "updated_at": datetime.utcnow()}},
            upsert=True
        )
    
    
    def get_fraud_alerts(
        self,
        user_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> List[Dict]:
        last_batch = self.get_last_batch_timestamp() or datetime.min
        end_time = end_time or datetime.utcnow()
        start_time = start_time or (end_time - timedelta(hours=24))
        
        results = []
        
        if start_time < last_batch:
            batch_query = {"is_fraud_predicted": True}
            if user_id:
                batch_query["user_id"] = user_id
            
            batch_results = list(
                self.batch_views.find(batch_query)
                    .sort("timestamp", DESCENDING)
                    .limit(limit)
            )
            results.extend(batch_results)
        
        if end_time >= last_batch:
            realtime_query = {"is_fraud_predicted": True}
            if user_id:
                realtime_query["user_id"] = user_id
            
            realtime_results = list(
                self.realtime_views.find(realtime_query)
                    .sort("processing_time", DESCENDING)
                    .limit(limit)
            )
            results.extend(realtime_results)
        
        for r in results:
            r.pop("_id", None)
        
        results.sort(key=lambda x: x.get("processing_time", x.get("timestamp", "")), reverse=True)
        return results[:limit]
    
    def get_user_profile(self, user_id: str) -> Optional[Dict]:
        profile = self.user_profiles.find_one({"user_id": user_id})
        if profile:
            profile.pop("_id", None)
        return profile
    
    def get_user_fraud_history(self, user_id: str, limit: int = 50) -> List[Dict]:
        return self.get_fraud_alerts(user_id=user_id, limit=limit)
    
    
    def get_fraud_rate_over_time(self, hours: int = 24) -> List[Dict]:
        pipeline = [
            {
                "$match": {
                    "timestamp": {
                        "$gte": datetime.utcnow() - timedelta(hours=hours)
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "$dateToString": {
                            "format": "%Y-%m-%d %H:00",
                            "date": "$timestamp"
                        }
                    },
                    "total": {"$sum": 1},
                    "fraud_count": {
                        "$sum": {"$cond": ["$is_fraud_predicted", 1, 0]}
                    },
                    "avg_score": {"$avg": "$fraud_score"}
                }
            },
            {"$sort": {"_id": 1}}
        ]
        
        return list(self.fraud_alerts.aggregate(pipeline))
    
    def get_transactions_by_country(self) -> List[Dict]:
        pipeline = [
            {
                "$group": {
                    "_id": "$country",
                    "count": {"$sum": 1},
                    "total_amount": {"$sum": "$amount"},
                    "fraud_count": {
                        "$sum": {"$cond": ["$is_fraud_predicted", 1, 0]}
                    }
                }
            },
            {"$sort": {"count": -1}},
            {"$limit": 20}
        ]
        
        results = list(self.realtime_views.aggregate(pipeline))
        if not results:
            results = list(self.batch_views.aggregate(pipeline))
        
        return results
    
    def get_high_risk_users(self, limit: int = 10) -> List[Dict]:
        pipeline = [
            {
                "$group": {
                    "_id": "$user_id",
                    "total_transactions": {"$sum": 1},
                    "fraud_count": {
                        "$sum": {"$cond": ["$is_fraud_predicted", 1, 0]}
                    },
                    "total_amount": {"$sum": "$amount"},
                    "avg_score": {"$avg": "$fraud_score"}
                }
            },
            {
                "$addFields": {
                    "fraud_rate": {
                        "$divide": ["$fraud_count", "$total_transactions"]
                    }
                }
            },
            {"$match": {"total_transactions": {"$gte": 3}}},
            {"$sort": {"fraud_rate": -1}},
            {"$limit": limit}
        ]
        
        results = list(self.fraud_alerts.aggregate(pipeline))
        return results
    
    def get_recent_stats(self) -> Dict:
        one_hour_ago = datetime.utcnow() - timedelta(hours=1)
        
        total_recent = self.realtime_views.count_documents({
            "processing_time": {"$gte": one_hour_ago.isoformat()}
        })
        
        fraud_recent = self.realtime_views.count_documents({
            "processing_time": {"$gte": one_hour_ago.isoformat()},
            "is_fraud_predicted": True
        })
        
        total_all = self.fraud_alerts.count_documents({})
        fraud_all = self.fraud_alerts.count_documents({"is_fraud_predicted": True})
        
        return {
            "last_hour": {
                "total": total_recent,
                "fraud": fraud_recent,
                "fraud_rate": fraud_recent / max(1, total_recent)
            },
            "all_time": {
                "total": total_all,
                "fraud": fraud_all,
                "fraud_rate": fraud_all / max(1, total_all)
            },
            "last_batch": self.get_last_batch_timestamp(),
            "timestamp": datetime.utcnow()
        }
    
    def get_live_dashboard_data(self) -> Dict:
        return {
            "stats": self.get_recent_stats(),
            "fraud_by_country": self.get_transactions_by_country(),
            "high_risk_users": self.get_high_risk_users(),
            "recent_alerts": self.get_fraud_alerts(limit=20)
        }
    
    
    def cleanup_old_realtime_views(self, hours: int = 2):
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        result = self.realtime_views.delete_many({
            "processing_time": {"$lt": cutoff.isoformat()}
        })
        print(f" Cleaned up {result.deleted_count} old real-time views")
        return result.deleted_count
    
    def get_layer_stats(self) -> Dict:
        return {
            "batch_views_count": self.batch_views.count_documents({}),
            "realtime_views_count": self.realtime_views.count_documents({}),
            "fraud_alerts_count": self.fraud_alerts.count_documents({}),
            "user_profiles_count": self.user_profiles.count_documents({}),
            "last_batch_timestamp": self.get_last_batch_timestamp()
        }
    
    def close(self):
        self.client.close()

def create_api():
    serving = ServingLayer()
    
    return {
        "get_fraud_alerts": serving.get_fraud_alerts,
        "get_user_profile": serving.get_user_profile,
        "get_user_fraud_history": serving.get_user_fraud_history,
        "get_fraud_rate_over_time": serving.get_fraud_rate_over_time,
        "get_transactions_by_country": serving.get_transactions_by_country,
        "get_high_risk_users": serving.get_high_risk_users,
        "get_recent_stats": serving.get_recent_stats,
        "get_live_dashboard_data": serving.get_live_dashboard_data,
        "get_layer_stats": serving.get_layer_stats,
        "_serving_layer": serving
    }

def main():
    print("=" * 60)
    print("SERVING LAYER TEST")
    print("=" * 60)
    
    serving = ServingLayer()
    
    print("\n Layer Statistics:")
    stats = serving.get_layer_stats()
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    print("\n Recent Fraud Alerts:")
    alerts = serving.get_fraud_alerts(limit=5)
    for alert in alerts[:5]:
        print(f"  - {alert.get('transaction_id', 'N/A')[:8]}... "
              f"User: {alert.get('user_id', 'N/A')} "
              f"Score: {alert.get('fraud_score', 0):.2f}")
    
    print("\n Transactions by Country:")
    by_country = serving.get_transactions_by_country()
    for item in by_country[:5]:
        print(f"  - {item['_id']}: {item['count']} transactions")
    
    print("\n High Risk Users:")
    risky = serving.get_high_risk_users(5)
    for user in risky:
        print(f"  - {user['_id']}: {user['fraud_rate']:.2%} fraud rate")
    
    serving.close()
    print("\n Serving layer test complete")

if __name__ == "__main__":
    main()
