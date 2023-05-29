FROM python:3-alpine

RUN pip install --upgrade pip
RUN pip install opcua

RUN mkdir /opcclient

WORKDIR /usr/src/opcclient
COPY opcuaclient.py .
EXPOSE 4840

CMD ["python","opcuaclient.py","--host","0.0.0.0"]