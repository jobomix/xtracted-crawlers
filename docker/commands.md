# Commands

##### start redis docker compose
```bash
cd <PROJECT_ROOT>/docker
docker compose up -d
```

##### start celery
```bash
cd <PROJECT_ROOT>
celery -A crawlers.tasks  worker --loglevel=INFO
```

##### send a task from python
```bash
cd <PROJECT_ROOT>
python -m crawlers.job_by_id
```
