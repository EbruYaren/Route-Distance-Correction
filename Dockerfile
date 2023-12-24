FROM python:3.7-slim

WORKDIR /usr/src/app

RUN pip3 install boto3==1.10.2 \
                 pymongo[tls,srv]==3.9 \
                 psycopg2-binary==2.8.4 \
                 pandas==0.25.2 \
                 requests==2.22.0 \
                 SQLAlchemy==1.3.10

ADD requirements.txt .
RUN pip3 install -r requirements.txt

ADD . .
ENTRYPOINT ["python", "-u", "main.py"]