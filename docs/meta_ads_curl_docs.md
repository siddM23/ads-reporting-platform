Last 7 days
curl -G \
  -d "level=campaign" \
  -d "fields=campaign_name,spend,results,website_purchase_roas,website_purchase_conversion_value" \
  -d "date_preset=last_7d" \
  -d "access_token=ACCESS_TOKEN" \
  "https://graph.facebook.com/API_VERSION/CAMPAIGN_ID/insights"

Last 30 days 
curl -G \
  -d "level=campaign" \
  -d "fields=campaign_name,spend,results,website_purchase_roas,website_purchase_conversion_value" \
  -d "date_preset=last_month" \
  -d "access_token=ACCESS_TOKEN" \
  "https://graph.facebook.com/API_VERSION/CAMPAIGN_ID/insights"

Last 6 months 
curl -G \
  -d "level=campaign" \
  -d "fields=campaign_name,spend,results,website_purchase_roas,website_purchase_conversion_value" \
  -d "date_preset=last_180d" \
  -d "access_token=ACCESS_TOKEN" \
  "https://graph.facebook.com/API_VERSION/CAMPAIGN_ID/insights"

