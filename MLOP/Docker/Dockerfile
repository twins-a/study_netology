FROM continuumio/miniconda3:latest

LABEL mainteiner="Aleks Tulyev @ twinsa@gm.com"

RUN ["mkdir", "./app"]

WORKDIR ./app

# copy app and files1.sh
# COPY ./app ./app
COPY /app/1.sh .


RUN chmod +x /app/1.sh

# python mlflow boto3 pymysql
RUN set -x pip install python
RUN set -x pip install mlflow boto3 pymysql

# Run Scripts
CMD ["./1.sh"]