FROM tiangolo/uvicorn-gunicorn:python3.10 AS carlemany-backend-base

RUN pip install --upgrade pip

COPY requirements/base.txt /tmp/requirements/

RUN pip install -r /tmp/requirements/base.txt

RUN mkdir /backend-backend

WORKDIR /carlemany-backend

COPY . ./

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "80", "--log-level", "debug", "--lifespan", "on"]
