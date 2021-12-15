FROM continuumio/miniconda3:latest

LABEL maintainer="Tulyev Aleks twinsa21@gmail.com"

WORKDIR /app

RUN pip install mlflow boto3 pymysql

COPY /app/1.sh .

RUN chmod +x /app/1.sh

# Run Scripts
# CMD ["./1.sh"]
CMD ["/app/1.sh"]