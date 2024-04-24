FROM  python:3.7

WORKDIR /usr/src/data_streaming 


COPY requirements.txt requirements.txt

RUN pip install --upgrade pip

RUN pip install -r requirements.txt 

CMD python --version && sleep infinity