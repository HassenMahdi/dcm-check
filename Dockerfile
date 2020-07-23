FROM frolvlad/alpine-python-machinelearning

# SETUP PYTHON ENV
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

RUN apk update && apk add postgresql-dev gcc python3-dev musl-dev libc-dev g++ libffi-dev libxml2 unixodbc-dev

RUN apk add build-base

WORKDIR /usr/src/app
COPY requirements.txt /usr/src/app/
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . /usr/src/app

EXPOSE 5002

ENTRYPOINT [ "gunicorn", "-w", "4", "app:app", "--bind", "0.0.0.0:5002"]