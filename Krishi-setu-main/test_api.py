import requests
import json

# Test the API search endpoint
base_url = "http://127.0.0.1:8008"

# Test 1: Search for "tractor subsidy"
print("Test 1: Search for 'tractor subsidy'")
response = requests.get(f"{base_url}/api/master/search", params={"query": "tractor subsidy", "scope": "trusted"})
print(f"Status: {response.status_code}")
data = response.json()
print(f"Total results: {data.get('total')}")
print(f"Items found: {len(data.get('items', []))}")
for item in data.get('items', []):
    print(f"  - {item.get('scheme_name')} (score: {item.get('smart_rank')})")

# Test 2: Filter by scheme_type
print("\nTest 2: Filter by subsidy type")
response = requests.get(f"{base_url}/api/master/search", params={"scheme_type": "subsidy", "scope": "trusted"})
print(f"Status: {response.status_code}")
data = response.json()
print(f"Total results: {data.get('total')}")
for item in data.get('items', []):
    print(f"  - {item.get('scheme_name')} ({item.get('scheme_type')})")

# Test 3: Get all schemes
print("\nTest 3: Get all schemes (no filter)")
response = requests.get(f"{base_url}/api/master/search", params={"scope": "trusted"})
print(f"Status: {response.status_code}")
data = response.json()
print(f"Total results: {data.get('total')}")
for item in data.get('items', [])[:5]:
    print(f"  - {item.get('scheme_name')}")

# Test 4: Search for "insurance"
print("\nTest 4: Search for 'insurance'")
response = requests.get(f"{base_url}/api/master/search", params={"query": "insurance", "scope": "trusted"})
print(f"Status: {response.status_code}")
data = response.json()
print(f"Total results: {data.get('total')}")
for item in data.get('items', []):
    print(f"  - {item.get('scheme_name')}")
