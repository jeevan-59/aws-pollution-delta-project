###FROM public.ecr.aws/lambda/python:3.8

FROM public.ecr.aws/lambda/python:3.11-x86_64

COPY app.py ${LAMBDA_TASK_ROOT}

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

CMD [ "app.handler" ]
