This file provides practical examples and commands for configuring, controlling, and troubleshooting services in your `docker-compose.yml`.  
It covers how to disable jobs by default using profiles, and includes best practices for managing optional or experimental jobs, plus essential Docker Compose commands for daily development and maintenance.

---

## 🟦 1. How to Disable Any Job (Example: forex)

You can disable any service in your `docker-compose.yml` by adding the `profiles: ["disabled"]` line to its definition.

For example, to disable the `forex` job by default, change its section to:

```yaml
forex:
  profiles: ["disabled"]
  build: ./forex
  container_name: forex
  # ...other config...
````

* Now, `forex` will **NOT start** unless you explicitly enable it.
* To run it manually:

```bash
docker compose --profile disabled up forex
```

> This method works for any job—just add the `profiles: ["disabled"]` line to the service you want to disable by default.

---

## 🟦 2. Useful Docker Compose Commands

### List all services and their status

```bash
docker compose ps
```

### Start only selected services

```bash
docker compose up servicename1 servicename2
```

### Stop all running services

```bash
docker compose down
```

### View logs for a specific service

```bash
docker compose logs servicename
```

### Rebuild images for all services

```bash
docker compose build
```

### Remove stopped containers, networks, and images

```bash
docker system prune -a
```

---

