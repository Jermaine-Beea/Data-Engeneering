This document contains useful commands for connecting to PostgreSQL, inspecting CRM logs, debugging containers, and performing maintenance tasks.

---

## 🟦 **0. CDR Usage API - HVS vs Postgres**

### **Default (Postgres only)**
```bash
docker compose up -d cdr-usage-api
```

### **With HVS enabled**
```bash
docker compose run -e USE_HVS=true cdr-usage-api
```

---

### **Additional HVS usage examples**

Below are multiple ways to enable HVS depending on your goal.

- Enable HVS for all services (set before `up`):

```bash
# export in your shell so `docker compose up` sees it
export USE_HVS=true
chmod +x reset-env.sh
./reset-env.sh

# or one-liner
USE_HVS=true ./reset-env.sh
```

- Enable HVS only for the one-off `cdr-usage-api` container (no change to `up`):

```bash
# run reset script (cleanup + up), then run that single service with HVS
chmod +x reset-env.sh
./reset-env.sh
docker compose run -e USE_HVS=true cdr-usage-api
```

- Make HVS permanent for the project (create `.env` so `docker compose up` picks it up):

```bash
echo "USE_HVS=true" > .env
chmod +x reset-env.sh
./reset-env.sh
```

- Make `reset-env.sh` export HVS by default (script-level change, allows override):

```bash
# add this line in `reset-env.sh` before `docker compose up -d`
export USE_HVS=${USE_HVS:-true}

# you can still override when running the script
USE_HVS=false ./reset-env.sh
```

---


---

## 🟦 **1. Connect to PostgreSQL**

### **Open a psql session**

```bash
sudo docker exec -it postgres psql -U postgres -d wtc_prod
```

---

## 🟦 **2. Useful SQL Queries**

### **Table row counts**

```sql
SELECT 
    (SELECT COUNT(*) FROM crm_system.accounts) AS accounts,
    (SELECT COUNT(*) FROM crm_system.addresses) AS addresses,
    (SELECT COUNT(*) FROM crm_system.devices) AS devices;
```

### **View sample records**

```sql
SELECT * FROM crm_system.accounts LIMIT 5;
```

### **View tables in schema**

```sql
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'crm_system';
```

### **Exit PostgreSQL**

```sql
\q
```

---

## 🟦 **3. Container Management**

### **Check running containers**

```bash
sudo docker ps
```

### **Stop and remove PostgreSQL (if conflicting)**

```bash
sudo docker stop postgres
sudo docker rm postgres
```

### **Restart CRM or Postgres**

```bash
sudo docker restart crm
sudo docker restart postgres
```

### **Open a shell inside a container**

```bash
sudo docker exec -it postgres bash
sudo docker exec -it crm bash
```

---

## 🟦 **4. Logs & Health Checks**

### **Watch CRM logs live**

```bash
sudo docker logs crm -f --tail 50
```

### **Watch Postgres logs live**

```bash
sudo docker logs postgres -f
```

### **Check PostgreSQL health**

```bash
sudo docker exec postgres pg_isready -U postgres
```

### **CRM logs with timestamps**

```bash
sudo docker logs crm --timestamps | tail -n 100
```

---

## 🟦 **5. Database Maintenance**

### **Backup the database**

```bash
sudo docker exec postgres pg_dump -U postgres wtc_prod > backup.sql
```

### **Restore from a backup**

```bash
sudo docker exec -i postgres psql -U postgres -d wtc_prod < backup.sql
```

### **Run a SQL script**

```bash
sudo docker exec -i postgres psql -U postgres -d wtc_prod < schema.sql
```

---

## 🟦 **6. Inspect & Diagnose**

### **Check container resource usage**

```bash
sudo docker stats
```

### **Inspect container configuration**

```bash
sudo docker inspect crm
sudo docker inspect postgres
```

### **View environment variables inside CRM**

```bash
sudo docker exec crm env
```

---

