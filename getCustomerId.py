from google.ads.googleads.client import GoogleAdsClient

# Initialize client without a login_customer_id in the config
client = GoogleAdsClient.load_from_storage("google-ads.yaml")
customer_service = client.get_service("CustomerService")

# 1. Get all base accounts you have access to
accessible_customers = customer_service.list_accessible_customers()

for resource_name in accessible_customers.resource_names:
    print(f"Direct Child/Account Found: {resource_name}")
    # resource_name looks like "customers/1234567890"               