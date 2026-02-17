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

Frontend :
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
