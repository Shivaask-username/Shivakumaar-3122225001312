import requests

url = "https://lists.blocklist.de/lists/ssh.txt"

try:
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    
    ip_list = [line.strip() for line in response.text.split('\n') 
               if line.strip() and not line.startswith('#')]
    
    print(f"✅ API access successful!")
    print(f"Retrieved {len(ip_list)} IP addresses")
    print(f"First 5 IPs: {ip_list[:5]}")
except Exception as e:
    print(f"❌ API test failed: {e}")
