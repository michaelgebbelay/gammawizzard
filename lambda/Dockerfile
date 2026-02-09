FROM public.ecr.aws/lambda/python:3.12

COPY lambda/requirements-lambda.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy trading scripts (unchanged)
COPY scripts/ ${LAMBDA_TASK_ROOT}/scripts/
COPY TT/ ${LAMBDA_TASK_ROOT}/TT/

# Copy handler
COPY lambda/handler.py ${LAMBDA_TASK_ROOT}/

CMD ["handler.lambda_handler"]
