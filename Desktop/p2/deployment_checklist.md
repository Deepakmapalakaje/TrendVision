# 🚀 DEPLOYMENT CHECKLIST FOR TRENDVISION2004.COM
## Ultra-Fast Trading System Production Deployment

---

## 📋 PRE-DEPLOYMENT CHECKLIST

### 🔧 **Files Preparation (VERIFY BEFORE DEPLOYMENT)**
- [ ] `app.py` - Main Flask application with optimized config loading
- [ ] `pipeline1.py` - Ultra-fast trading pipeline (dual-CPU optimized)
- [ ] `config/config.json` - Configuration with valid Upstox credentials
- [ ] `requirements.txt` - All Python dependencies specified
- [ ] `database/users.db` - User database (optional for fresh deployment)
- [ ] Static files in `static/` folder
- [ ] Template files in `templates/` folder
- [ ] `MarketDataFeedV3_pb2.py` - Protobuf definitions
- [ ] `MarketDataFeedV3.proto` - Protocol buffer source

### 🌐 **Domain & DNS Configuration**
- [ ] Domain `trendvision2004.com` purchased and accessible
- [ ] DNS A record pointing to VM IP: `34.180.14.162`
- [ ] DNS CNAME record for `www.trendvision2004.com`
- [ ] DNS propagation completed (verify with `nslookup`)

### ☁️ **Google Cloud VM Setup**
- [ ] VM instance created: `instance-20250916-202625`
- [ ] Machine type: `c4-standard-2` (2 vCPUs, 7GB RAM)
- [ ] Debian 12 OS installed and running
- [ ] SSH access configured and tested
- [ ] Firewall rules for HTTP/HTTPS enabled

### 🔧 **Configuration Files Validated**
- [ ] `config.json` contains:
  - [ ] Valid `ACCESS_TOKEN`
  - [ ] `NIFTY_FUTURE_key`
  - [ ] `ITM_CE_key` and `ITM_CE_strike`
  - [ ] `ITM_PE_key` and `ITM_PE_strike`
- [ ] Database paths correctly configured for Linux environment
- [ ] Port 8080 available for Flask application

---

## 🚀 DEPLOYMENT STEPS

### Phase 1: VM Preparation
- [ ] SSH into VM: `ssh username@34.180.14.162`
- [ ] Update system: `sudo apt update && sudo apt upgrade -y`

### Phase 2: File Transfer
- [ ] Create deployment directory: `mkdir -p /home/trading_app`
- [ ] Transfer all project files:
  ```bash
  scp -r * username@34.180.14.162:/home/trading_app/
  ```
- [ ] Set proper file permissions:
  ```bash
  sudo chown -R $USER:$USER /home/trading_app/
  chmod -R 755 /home/trading_app/
  ```

### Phase 3: Production Setup
- [ ] Make deployment script executable:
  ```bash
  chmod +x deploy.sh
  ```
- [ ] Run deployment script:
  ```bash
  ./deploy.sh
  ```
- [ ] Verify all checks pass
- [ ] Confirm services are running:
  ```bash
  sudo systemctl status trading-app
  sudo systemctl status nginx
  ```

### Phase 4: Domain & SSL Setup
- [ ] Update DNS records (A record -> VM IP)
- [ ] Wait for DNS propagation (15-30 minutes)
- [ ] Run SSL setup:
  ```bash
  chmod +x domain_setup.sh
  ./domain_setup.sh
  ```
- [ ] Test HTTPS access: `https://trendvision2004.com`

---

## 🔍 POST-DEPLOYMENT VERIFICATION

### Service Verification
- [ ] HTTP to HTTPS redirect working
- [ ] Flask application responding on port 8080
- [ ] Nginx proxy correctly configured
- [ ] WebSocket support enabled for real-time data
- [ ] Static files served correctly

### Application Verification
- [ ] Login page loads: `https://trendvision2004.com/login`
- [ ] Registration system functional
- [ ] Dashboard accessible after login
- [ ] Real-time data streaming working
- [ ] Trading signals displayed correctly

### Performance Verification
- [ ] Page load times < 2 seconds
- [ ] WebSocket connections stable
- [ ] Memory usage within limits (< 6GB)
- [ ] Database operations performing (commit intervals < 0.5s)

### Security Verification
- [ ] SSL certificate valid (Let's Encrypt)
- [ ] Firewall correctly configured
- [ ] Only required ports open (80, 443, 8080)
- [ ] No security vulnerabilities detected

---

## 🎯 PRODUCTION CHECKLIST

### Day 1 - Launch
- [ ] Initial deployment completed successfully
- [ ] Domain pointing correctly
- [ ] SSL certificate installed
- [ ] Basic functionality working
- [ ] Backup of configuration files created

### Week 1 - Optimization
- [ ] Monitor application logs for errors
- [ ] Check performance metrics daily
- [ ] Verify trading pipeline processing speed
- [ ] Test market hours auto-start
- [ ] Review memory and CPU usage

### Month 1 - Stabilization
- [ ] Implement user registration system
- [ ] Test notification emails (OTP system)
- [ ] Verify database backup procedures
- [ ] Review SSL auto-renewal system
- [ ] Check automated maintenance schedules

---

## 🚨 TROUBLESHOOTING GUIDE

### Common Issues & Solutions

**Issue: Port 8080 already in use**
```bash
# Find process using port 8080
sudo lsof -i :8080
sudo kill -9 <PID>
```

**Issue: Flask app not starting**
```bash
# Check logs
sudo journalctl -u trading-app
# Test manually
cd /home/trading_app
source trading_env/bin/activate
python app.py
```

**Issue: Nginx configuration error**
```bash
# Test configuration
sudo nginx -t
# Reload configuration
sudo systemctl reload nginx
```

**Issue: SSL certificate not renewing**
```bash
# Manual renewal
sudo certbot renew
# Test renewal
sudo certbot renew --dry-run
```

**Issue: Database connection errors**
```bash
# Check database file permissions
ls -la /home/trading_app/database/
sudo chown $USER:$USER /home/trading_app/database/*.db
```

**Issue: WebSocket connections failing**
```bash
# Check firewall rules
sudo firewall-cmd --list-all
sudo firewall-cmd --permanent --add-port=8080/tcp
sudo firewall-cmd --reload
```

---

## 📊 MONITORING COMMANDS

### Service Status
```bash
sudo systemctl status trading-app nginx
```

### Logs
```bash
# Application logs
sudo journalctl -u trading-app -f
# Nginx logs
sudo journalctl -u nginx -f
# System logs
sudo journalctl -f
```

### Performance Monitoring
```bash
# System resources
htop
# Network connections
nload
# Database status
sudo systemctl status sqlite3
```

### SSL Status
```bash
# SSL certificate info
openssl s_client -connect trendvision2004.com:443 2>/dev/null | openssl x509 -noout -dates -subject
# Renewal test
sudo certbot renew --dry-run
```

---

## 🔄 BACKUP & RECOVERY

### Automatic Backups (Configured)
- Daily backups at 2-3 AM
- Configuration and databases backed up
- Logs archived with compression

### Manual Backup
```bash
# Create full backup
sudo systemctl stop trading-app
tar -czf trading_backup_$(date +%Y%m%d_%H%M%S).tar.gz /home/trading_app/
sudo systemctl start trading-app
```

### Emergency Recovery
1. Stop all services: `sudo systemctl stop trading-app nginx`
2. Restore from backup
3. Reconfigure SSL
4. Restart services

---

## 📞 SUPPORT CONTACTS

**Deployment Issues:**
-
**Domain Configuration:**
- Domain registrar support

**Google Cloud Support:**
- GCP Console → Support

**SSL Certificate Issues:**
- [Let's Encrypt Documentation](https://letsencrypt.org/docs/)

---

*✅ THIS CHECKLIST MUST BE COMPLETED BEFORE GOING LIVE*
