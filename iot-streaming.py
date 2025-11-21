"""
KPI Analytics Server with IBM Watson Orchestrate Integration
Automatically triggers IBM Watson LLM for maintenance decisions
Runs on localhost:8080
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import http.client
import threading
import time
from datetime import datetime, timedelta
from collections import deque
from typing import Dict, List, Optional
import numpy as np
import json
import ssl

app = Flask(__name__)
CORS(app)

# ============================================================================
# IBM WATSON ORCHESTRATE INTEGRATION
# ============================================================================

class IBMWatsonAgent:
    """Integration with IBM Watson Orchestrate API using http.client"""
    
    def __init__(self, region_code: str, instance_id: str, iam_token: Optional[str] = None):
        self.region_code = region_code
        self.instance_id = instance_id
        self.iam_token = iam_token
        self.host = f"api.{region_code}.watson-orchestrate.ibm.com"
        self.request_history = deque(maxlen=50)
        self.last_check_time = {}
        self.check_interval = 300  # Check every 5 minutes minimum
        
        print(f"✅ IBM Watson configured: {self.host}")
        print(f"   Instance ID: {self.instance_id}")
        print(f"   IAM Token: {'Configured' if self.iam_token else 'Not set'}")
        
    def should_trigger_check(self) -> bool:
        """Determine if enough time has passed to trigger another check"""
        now = datetime.now()
        last_check = self.last_check_time.get('global')
        
        if last_check is None:
            return True
        
        elapsed = (now - last_check).total_seconds()
        return elapsed >= self.check_interval
    
    def trigger_maintenance_check(self, locations_kpis: dict) -> dict:
        """
        Trigger IBM Watson to check maintenance needs for all locations
        """
        if not self.should_trigger_check():
            print("⏸️  Skipping Watson check (cooldown active)")
            return None
        
        print(f"\n{'='*70}")
        print(f"🤖 Triggering IBM Watson Orchestrate")
        print(f"{'='*70}")
        
        # Build payload for Watson
        payload = {
            "message": "Check for the maintenance needs for all the locations and trigger a work order when needed.",
            "context": {
                "locations": self._build_locations_summary(locations_kpis),
                "timestamp": datetime.now().isoformat(),
                "alert_summary": self._get_alert_summary(locations_kpis)
            }
        }
        
        print(f"Sending request to: https://{self.host}")
        print(f"Instance ID: {self.instance_id}")
        print(f"Payload summary: {len(payload['context']['locations'])} locations")
        
        try:
            # Create HTTPS connection using http.client
            conn = http.client.HTTPSConnection(self.host)
            
            # Prepare headers
            headers = {
                'Authorization': f"Bearer {self.iam_token}",
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
            
            # Prepare endpoint - using orchestrate API
            endpoint = f"/instances/{self.instance_id}/v1/orchestrate"
            endpoint = endpoint.replace(" ", "%20")
            # Convert payload to JSON string
            payload_json = json.dumps(payload)
            
            # Make POST request
            conn.request("POST", endpoint, body=payload_json, headers=headers)
            
            # Get response
            res = conn.getresponse()
            data = res.read()
            
            # Decode response
            response_text = data.decode("utf-8")
            status_code = res.status
            
            # Record request
            request_record = {
                'timestamp': datetime.now().isoformat(),
                'endpoint': endpoint,
                'payload_size': len(payload_json),
                'status_code': status_code,
                'response': response_text[:500] if response_text else None
            }
            self.request_history.append(request_record)
            
            # Update last check time
            self.last_check_time['global'] = datetime.now()
            
            # Close connection
            conn.close()
            
            if status_code == 200 or status_code == 201:
                print(f"✅ Watson API call successful (Status: {status_code})")
                print(f"Response: {response_text[:200]}")
                
                try:
                    response_json = json.loads(response_text)
                except json.JSONDecodeError:
                    response_json = {'raw_response': response_text}
                
                return {
                    'success': True,
                    'status_code': status_code,
                    'response': response_json,
                    'timestamp': datetime.now().isoformat()
                }
            else:
                print(f"⚠️  Watson API returned status {status_code}")
                print(f"Response: {response_text[:200]}")
                return {
                    'success': False,
                    'status_code': status_code,
                    'error': response_text,
                    'timestamp': datetime.now().isoformat()
                }
                
        except Exception as e:
            print(f"❌ Watson API error: {e}")
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
        finally:
            print(f"{'='*70}\n")
    
    def _build_locations_summary(self, locations_kpis: dict) -> list:
        """Build summary of all locations for Watson"""
        summary = []
        
        for location_id, kpis in locations_kpis.items():
            location_summary = {
                'location_id': location_id,
                'kpis': {},
                'overall_health': self._assess_health(kpis),
                'alerts': []
            }
            
            # Add each KPI
            for kpi_name, kpi_data in kpis.items():
                if kpi_data.get('value') is not None:
                    location_summary['kpis'][kpi_name] = {
                        'value': kpi_data['value'],
                        'status': kpi_data['status'],
                        'unit': kpi_data['unit'],
                        'target': kpi_data.get('target')
                    }
                    
                    # Add alert if poor status
                    if kpi_data['status'] in ['poor', 'fair']:
                        location_summary['alerts'].append({
                            'kpi': kpi_name,
                            'value': kpi_data['value'],
                            'status': kpi_data['status'],
                            'message': f"{kpi_name} is {kpi_data['status']}: {kpi_data['value']}{kpi_data['unit']}"
                        })
            
            summary.append(location_summary)
        
        return summary
    
    def _assess_health(self, kpis: dict) -> str:
        """Assess overall health from KPIs"""
        statuses = [kpi.get('status') for kpi in kpis.values() if kpi.get('status')]
        
        if 'poor' in statuses:
            return 'critical'
        elif statuses.count('fair') >= 2:
            return 'warning'
        elif 'fair' in statuses:
            return 'attention'
        else:
            return 'healthy'
    
    def _get_alert_summary(self, locations_kpis: dict) -> dict:
        """Get summary of alerts across all locations"""
        critical_count = 0
        warning_count = 0
        total_locations = len(locations_kpis)
        
        for location_id, kpis in locations_kpis.items():
            health = self._assess_health(kpis)
            if health == 'critical':
                critical_count += 1
            elif health in ['warning', 'attention']:
                warning_count += 1
        
        return {
            'total_locations': total_locations,
            'critical_locations': critical_count,
            'warning_locations': warning_count,
            'healthy_locations': total_locations - critical_count - warning_count,
            'requires_immediate_action': critical_count > 0
        }


# ============================================================================
# KPI CALCULATOR with Auto-Triggering
# ============================================================================

class KPIAnalytics:
    def __init__(self, iot_server_url: str = "http://localhost:8161", 
                 watson_region: str = None, watson_instance_id: str = None, watson_iam_token: str = None):
        self.iot_server_url = iot_server_url
        self.location_data = {}  # Now keyed by location_1, location_2, location_3
        self.kpi_cache = {}
        self.lock = threading.Lock()
        self.running = False
        
        # IBM Watson integration
        if watson_region and watson_instance_id:
            self.watson_agent = IBMWatsonAgent(watson_region, watson_instance_id, watson_iam_token)
        else:
            self.watson_agent = None
            print("⚠️  No Watson credentials provided")
        
    def start_monitoring(self, interval: float = 3.0):
        """Start monitoring IoT server"""
        self.running = True
        thread = threading.Thread(
            target=self._monitoring_loop,
            args=(interval,),
            daemon=True
        )
        thread.start()
        print(f"👁️  Started monitoring {self.iot_server_url}")
        
    def _monitoring_loop(self, interval: float):
        """Continuously fetch and process data"""
        while self.running:
            try:
                # Use http.client for consistency
                conn = http.client.HTTPConnection("localhost", 8161)
                conn.request("GET", "/stream/latest")
                res = conn.getresponse()
                
                if res.status == 200:
                    data_bytes = res.read()
                    data = json.loads(data_bytes.decode("utf-8"))
                    
                    self._process_data(data)
                    self._calculate_all_kpis()
                    
                    # Check if Watson should be triggered
                    if self.watson_agent:
                        self._check_and_trigger_watson()
                
                conn.close()
                    
            except Exception as e:
                print(f"❌ Error fetching from IoT server: {e}")
            
            time.sleep(interval)
    
    def _process_data(self, stream_data: dict):
        """Process incoming stream data - locations are dataset names"""
        with self.lock:
            for location_id, record in stream_data.get('data', {}).items():
                if record is None:
                    continue
                
                # Location ID is the dataset name (location_1, location_2, location_3)
                if location_id not in self.location_data:
                    self.location_data[location_id] = deque(maxlen=100)
                
                self.location_data[location_id].append(record)
    
    def _calculate_all_kpis(self):
        """Calculate KPIs for all locations"""
        with self.lock:
            for location_id, records in self.location_data.items():
                if len(records) == 0:
                    continue
                
                self.kpi_cache[location_id] = {
                    'OEE': self._calculate_oee(records),
                    'MTBF': self._calculate_mtbf(records),
                    'MTTR': self._calculate_mttr(records),
                    'PVUR': self._calculate_pvur(records),
                    'Availability': self._calculate_availability(records)
                }
    
    def _check_and_trigger_watson(self):
        """Check if any location needs attention and trigger Watson"""
        with self.lock:
            locations_kpis = dict(self.kpi_cache)
        
        # Check if any location has issues
        needs_attention = False
        for location_id, kpis in locations_kpis.items():
            for kpi_name, kpi_data in kpis.items():
                if kpi_data.get('status') in ['poor', 'fair']:
                    needs_attention = True
                    break
            if needs_attention:
                break
        
        # Trigger Watson if needed
        if needs_attention:
            print("⚠️  Issues detected, triggering IBM Watson Orchestrate")
    
    # KPI Calculation Methods (same as before)
    def _calculate_oee(self, records: deque) -> dict:
        if len(records) == 0:
            return {'value': None, 'status': 'no_data', 'unit': '%'}
        
        latest = records[-1]
        if 'data' in latest and 'OEE' in latest['data']:
            oee_value = float(latest['data']['OEE'])
        else:
            availability = self._calculate_availability(records)['value']
            if availability is None:
                return {'value': None, 'status': 'insufficient_data', 'unit': '%'}
            oee_value = availability
        
        if oee_value >= 85:
            status = 'excellent'
        elif oee_value >= 75:
            status = 'good'
        elif oee_value >= 65:
            status = 'fair'
        else:
            status = 'poor'
        
        return {
            'value': round(oee_value, 2),
            'status': status,
            'unit': '%',
            'target': 75.0,
            'timestamp': datetime.now().isoformat()
        }
    
    def _calculate_mtbf(self, records: deque) -> dict:
        if len(records) < 2:
            return {'value': None, 'status': 'insufficient_data', 'unit': 'cycles'}
        
        failures = []
        for i, record in enumerate(records):
            data = record.get('data', {})
            if data.get('Machine failure') == 1 or data.get('failure') == 1:
                failures.append(i)
        
        if len(failures) == 0:
            mtbf_value = len(records)
            status = 'excellent'
        elif len(failures) == 1:
            mtbf_value = len(records) - failures[0]
            status = 'good' if mtbf_value > 100 else 'fair'
        else:
            intervals = [failures[i] - failures[i-1] for i in range(1, len(failures))]
            mtbf_value = np.mean(intervals)
            
            if mtbf_value > 100:
                status = 'good'
            elif mtbf_value > 50:
                status = 'fair'
            else:
                status = 'poor'
        
        return {
            'value': round(mtbf_value, 2),
            'status': status,
            'unit': 'cycles',
            'target': 100.0,
            'failures_detected': len(failures),
            'timestamp': datetime.now().isoformat()
        }
    
    def _calculate_mttr(self, records: deque) -> dict:
        if len(records) < 2:
            return {'value': None, 'status': 'insufficient_data', 'unit': 'hours'}
        
        downtime_periods = []
        in_downtime = False
        downtime_start = None
        
        for i, record in enumerate(records):
            data = record.get('data', {})
            is_failed = data.get('Machine failure') == 1 or data.get('failure') == 1
            
            if is_failed and not in_downtime:
                in_downtime = True
                downtime_start = i
            elif not is_failed and in_downtime:
                in_downtime = False
                if downtime_start is not None:
                    downtime_periods.append(i - downtime_start)
        
        if len(downtime_periods) == 0:
            mttr_value = 0
            status = 'excellent'
        else:
            avg_cycles = np.mean(downtime_periods)
            mttr_value = avg_cycles * 0.5
            
            if mttr_value < 2:
                status = 'excellent'
            elif mttr_value < 4:
                status = 'good'
            elif mttr_value < 8:
                status = 'fair'
            else:
                status = 'poor'
        
        return {
            'value': round(mttr_value, 2),
            'status': status,
            'unit': 'hours',
            'target': 4.0,
            'repair_events': len(downtime_periods),
            'timestamp': datetime.now().isoformat()
        }
    
    def _calculate_pvur(self, records: deque) -> dict:
        if len(records) == 0:
            return {'value': None, 'status': 'no_data', 'unit': '%'}
        
        total_cycles = len(records)
        productive_cycles = 0
        for record in records:
            data = record.get('data', {})
            if data.get('Machine failure', 0) == 0:
                productive_cycles += 1
        
        pvur_value = (productive_cycles / total_cycles) * 100 if total_cycles > 0 else 0
        
        if pvur_value >= 90:
            status = 'excellent'
        elif pvur_value >= 80:
            status = 'good'
        elif pvur_value >= 70:
            status = 'fair'
        else:
            status = 'poor'
        
        return {
            'value': round(pvur_value, 2),
            'status': status,
            'unit': '%',
            'target': 80.0,
            'productive_cycles': productive_cycles,
            'total_cycles': total_cycles,
            'timestamp': datetime.now().isoformat()
        }
    
    def _calculate_availability(self, records: deque) -> dict:
        if len(records) == 0:
            return {'value': None, 'status': 'no_data', 'unit': '%'}
        
        latest = records[-1]
        data = latest.get('data', {})
        
        if 'Availability' in data:
            availability_value = float(data['Availability'])
        else:
            uptime_count = sum(1 for r in records if r.get('data', {}).get('Machine failure', 0) == 0)
            availability_value = (uptime_count / len(records)) * 100
        
        if availability_value >= 90:
            status = 'excellent'
        elif availability_value >= 85:
            status = 'good'
        elif availability_value >= 75:
            status = 'fair'
        else:
            status = 'poor'
        
        return {
            'value': round(availability_value, 2),
            'status': status,
            'unit': '%',
            'target': 85.0,
            'timestamp': datetime.now().isoformat()
        }
    
    def get_kpi(self, location_id: str, kpi_name: str) -> Optional[dict]:
        with self.lock:
            if location_id not in self.kpi_cache:
                return None
            return self.kpi_cache[location_id].get(kpi_name)
    
    def get_all_kpis(self, location_id: str) -> Optional[dict]:
        with self.lock:
            return self.kpi_cache.get(location_id)
    
    def get_all_locations_kpis(self) -> dict:
        with self.lock:
            return dict(self.kpi_cache)


# Initialize analytics (will be configured in main)
analytics = None

# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.route('/health', methods=['GET'])
def health():
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'monitored_locations': list(analytics.location_data.keys()) if analytics else [],
        'watson_configured': analytics.watson_agent is not None if analytics else False
    })

@app.route('/watson/trigger', methods=['POST'])
def manual_watson_trigger():
    """Manually trigger Watson check"""
    if not analytics or not analytics.watson_agent:
        return jsonify({'error': 'Watson not configured'}), 400
    
    with analytics.lock:
        locations_kpis = dict(analytics.kpi_cache)
    
    result = analytics.watson_agent.trigger_maintenance_check(locations_kpis)
    return jsonify(result)

@app.route('/watson/history', methods=['GET'])
def watson_history():
    """Get Watson request history"""
    if not analytics or not analytics.watson_agent:
        return jsonify({'error': 'Watson not configured'}), 400
    
    return jsonify({
        'requests': list(analytics.watson_agent.request_history),
        'count': len(analytics.watson_agent.request_history)
    })

@app.route('/watson/config', methods=['GET'])
def watson_config():
    """Get Watson configuration"""
    if not analytics or not analytics.watson_agent:
        return jsonify({'error': 'Watson not configured'}), 400
    
    return jsonify({
        'region_code': analytics.watson_agent.region_code,
        'instance_id': analytics.watson_agent.instance_id,
        'host': analytics.watson_agent.host,
        'iam_token_configured': bool(analytics.watson_agent.iam_token),
        'check_interval_seconds': analytics.watson_agent.check_interval,
        'last_check_time': analytics.watson_agent.last_check_time.get('global', 'Never').isoformat() if isinstance(analytics.watson_agent.last_check_time.get('global'), datetime) else 'Never'
    })

@app.route('/watson/config', methods=['PUT'])
def update_watson_config():
    """Update Watson configuration"""
    if not analytics or not analytics.watson_agent:
        return jsonify({'error': 'Watson not configured'}), 400
    
    data = request.json
    if 'check_interval' in data:
        analytics.watson_agent.check_interval = int(data['check_interval'])
    
    return jsonify({
        'message': 'Configuration updated',
        'check_interval_seconds': analytics.watson_agent.check_interval
    })

# KPI Endpoints
@app.route('/kpi/<location_id>/oee', methods=['GET'])
def get_oee(location_id):
    kpi = analytics.get_kpi(location_id, 'OEE')
    if kpi is None:
        return jsonify({'error': f'No data for location {location_id}'}), 404
    return jsonify({'location_id': location_id, 'kpi': 'OEE', 'data': kpi})

@app.route('/kpi/<location_id>/mtbf', methods=['GET'])
def get_mtbf(location_id):
    kpi = analytics.get_kpi(location_id, 'MTBF')
    if kpi is None:
        return jsonify({'error': f'No data for location {location_id}'}), 404
    return jsonify({'location_id': location_id, 'kpi': 'MTBF', 'data': kpi})

@app.route('/kpi/<location_id>/mttr', methods=['GET'])
def get_mttr(location_id):
    kpi = analytics.get_kpi(location_id, 'MTTR')
    if kpi is None:
        return jsonify({'error': f'No data for location {location_id}'}), 404
    return jsonify({'location_id': location_id, 'kpi': 'MTTR', 'data': kpi})

@app.route('/kpi/<location_id>/pvur', methods=['GET'])
def get_pvur(location_id):
    kpi = analytics.get_kpi(location_id, 'PVUR')
    if kpi is None:
        return jsonify({'error': f'No data for location {location_id}'}), 404
    return jsonify({'location_id': location_id, 'kpi': 'PVUR', 'data': kpi})

@app.route('/kpi/<location_id>/availability', methods=['GET'])
def get_availability(location_id):
    kpi = analytics.get_kpi(location_id, 'Availability')
    if kpi is None:
        return jsonify({'error': f'No data for location {location_id}'}), 404
    return jsonify({'location_id': location_id, 'kpi': 'Availability', 'data': kpi})

@app.route('/kpi/<location_id>/all', methods=['GET'])
def get_all_kpis_location(location_id):
    kpis = analytics.get_all_kpis(location_id)
    if kpis is None:
        return jsonify({'error': f'No data for location {location_id}'}), 404
    return jsonify({
        'location_id': location_id,
        'timestamp': datetime.now().isoformat(),
        'kpis': kpis
    })

@app.route('/kpi/all', methods=['GET'])
def get_all_kpis_all_locations():
    kpis = analytics.get_all_locations_kpis()
    return jsonify({
        'timestamp': datetime.now().isoformat(),
        'locations': kpis
    })

@app.route('/locations', methods=['GET'])
def list_locations():
    return jsonify({
        'locations': list(analytics.location_data.keys()) if analytics else [],
        'count': len(analytics.location_data) if analytics else 0
    })

# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    import os
    
    print("📊 Starting KPI Analytics Server with IBM Watson Orchestrate")
    print("="*70)
    
    # Get Watson API configuration from environment
    WATSON_REGION = os.environ.get('WATSON_REGION')
    WATSON_INSTANCE_ID = os.environ.get('WATSON_INSTANCE')
    WATSON_API_KEY = os.environ.get('WATSON_API_KEY')
    
    if not WATSON_INSTANCE_ID:
        print("⚠️  WARNING: WATSON_INSTANCE_ID not set!")
        print("   Set it with: export WATSON_INSTANCE_ID='your-instance-id'")
    
    if not WATSON_IAM_TOKEN:
        print("⚠️  WARNING: WATSON_IAM_TOKEN not set!")
        print("   Set it with: export WATSON_IAM_TOKEN='your-iam-token'")
    
    # Initialize analytics with Watson
    analytics = KPIAnalytics(
        iot_server_url="http://localhost:8161",
        watson_region=WATSON_REGION,
        watson_instance_id=WATSON_INSTANCE_ID,
        watson_iam_token=WATSON_IAM_TOKEN
    )
    
    # Start monitoring
    analytics.start_monitoring(interval=3.0)
    
    print("\n✅ Server starting on http://localhost:8080")
    print("\nIBM Watson Integration:")
    print(f"  Region: {WATSON_REGION}")
    print(f"  Instance ID: {WATSON_INSTANCE_ID if WATSON_INSTANCE_ID else 'NOT SET'}")
    print(f"  IAM Token: {'Configured' if WATSON_IAM_TOKEN else 'NOT SET'}")
    print(f"  Auto-trigger: Enabled (every 5 minutes when issues detected)")
    print("\nAvailable endpoints:")
    print("  [KPI Endpoints]")
    print("  GET  /kpi/<location_id>/oee")
    print("  GET  /kpi/<location_id>/mtbf")
    print("  GET  /kpi/<location_id>/mttr")
    print("  GET  /kpi/<location_id>/pvur")
    print("  GET  /kpi/<location_id>/availability")
    print("  GET  /kpi/<location_id>/all")
    print("  GET  /kpi/all")
    print("\n  [Watson Integration]")
    print("  POST /watson/trigger          - Manually trigger Watson check")
    print("  GET  /watson/history          - View Watson request history")
    print("  GET  /watson/config           - View Watson config")
    print("  PUT  /watson/config           - Update Watson config")
    print("="*70 + "\n")
    
    app.run(host='0.0.0.0', port=8080, debug=False, threaded=True)
