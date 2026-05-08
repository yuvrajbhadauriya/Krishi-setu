import requests
import json

base_url = "http://127.0.0.1:8008"

# Test with the same parameters the finder would use
print("Test 1: Using /api/schemes (what finder uses)")
response = requests.get(f"{base_url}/api/schemes", params={"query": "tractor subsidy", "scope": "curated"})
print(f"Status: {response.status_code}")
data = response.json()
print(f"Total results: {data.get('total')}")
print(f"Items found: {len(data.get('items', []))}")
for item in data.get('items', [])[:5]:
    print(f"  - {item.get('scheme_name')}")

# Test with /api/master/search for comparison
print("\nTest 2: Using /api/master/search (what we tested before)")
response = requests.get(f"{base_url}/api/master/search", params={"query": "tractor subsidy", "scope": "trusted"})
print(f"Status: {response.status_code}")
data = response.json()
print(f"Total results: {data.get('total')}")
print(f"Items found: {len(data.get('items', []))}")
for item in data.get('items', [])[:5]:
    print(f"  - {item.get('scheme_name')}")

# Test with scheme_type filter like finder does
print("\nTest 3: Filter by scheme_type=subsidy using /api/schemes")
response = requests.get(f"{base_url}/api/schemes", params={"scheme_type": "subsidy", "scope": "curated"})
print(f"Status: {response.status_code}")
data = response.json()
print(f"Total results: {data.get('total')}")
for item in data.get('items', [])[:5]:
    print(f"  - {item.get('scheme_name')}")
