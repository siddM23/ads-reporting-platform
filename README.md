# CUBE-Ads Reporting Platform

## 1. System Setup

```bash
sudo apt install -y nginx python3-pip python3-venv git certbot python3-certbot-nginx
```
### run scripts/setup_db.py # Create tables on dynamodb


## 2. Install Node.js (via NVM)
```bash
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash
source ~/.bashrc
nvm install 20
nvm use 20
```

## 3. Install PM2
```bash
npm install -g pm2
```
## 4. System


Frontend : Node(Next js)
Backend : Python(FastAPI)
Database : DynamoDB

### Frontend

cd frontend
npm install
npm run build
npm start


### Backend Setup
```bash
cd api
pip install -r requirements.txt
pip install uvicorn
uvicorn entry:app --host 0.0.0.0/[IP_ADDRESS] --port 8000 --reload

## 6. Configure PM2 (Process Management)
PM2 keeps your apps running in the background. From the **project root**:
```bash
pm2 start ecosystem.config.js
pm2 save
pm2 startup
```

## 7. Configure Nginx
```bash
sudo cp nginx.conf /etc/nginx/sites-available/cube-arp
sudo ln -s /etc/nginx/sites-available/cube-arp /etc/nginx/sites-enabled/
sudo rm /etc/nginx/sites-enabled/default  # Remove default config
sudo nginx -t  # Test configuration
sudo systemctl restart nginx
```

## 8. Setup SSL (HTTPS)
Run Certbot to automatically configure SSL and force HTTPS:
```bash
sudo certbot --nginx -d your-domain.com -d www.your-domain.com
```

---

## Troubleshooting
- **Check Logs**: `pm2 logs`
- **Nginx Errors**: `sudo tail -f /var/log/nginx/error.log`
- **App Status**: `pm2 status`
- **Restart All**: `pm2 restart all`




## DOCS: Dynamo DB update and required Future works

This commit reflects the recent DB improvements to the ARP (Ads Reporting Platform). 


## Improvements Made:
- UI Documentation: Added purpose-driven comments to the core Shad-CN UI library 
  (Badge, Button, Card, Popover, DateFilter) to improve developer onboarding and code maintainability.
- Identity Migration: Successfully migrated Users and Integrations tables from email-based 
  Primary Keys to UUID-based IDs.

- Soft Deletion: Integrated 'is_deleted' logic for Integrations to prevent accidental data 
  loss.

- Backend Isolation: Refactored the `/api/insights` endpoints to explicitly filter results 
  by authorized integration IDs retrieved from the user session (Problem: This shouldn't be done of the backend, the DB pull takes longer than needed and filtering out integrations adds time to this, needs change in DB design).

## Current Limitations:
- Backend Filtering Latency: Authorization filtering for ads data is handled on the backend, leading to inefficient database scans and increased API response times.
- Storage Growth: Currently lacks a TTL (Time To Live) strategy, meaning stale metric data will grow indefinitely. 

## Future Work:
- Schema Hardening: Recreate Metrics tables using 'integration_id' as the Partition Key 
  for physical data isolation (Silo Pattern).
- Performance Indexing: Implement 'IntegrationRangeIndex' (PK: integration_id, SK: range_days) 
  to eliminate hot partitions and achieve sub-30ms dashboard loads.
- Lifecycle Management: Add TTL attributes to metric records to automate the cleanup 
  of data older than 180 days.
- Unified Metrics: Consolidate platform-specific tables into a single 'Ad_Metrics' table 
  to simplify cross-channel reporting logic.