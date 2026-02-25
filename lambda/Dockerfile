FROM public.ecr.aws/lambda/python:3.12

COPY lambda/requirements-lambda.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy trading scripts (unchanged)
COPY scripts/ ${LAMBDA_TASK_ROOT}/scripts/
COPY TT/ ${LAMBDA_TASK_ROOT}/TT/

# Copy sim data layer (chain collection, no heavy deps)
COPY sim/__init__.py ${LAMBDA_TASK_ROOT}/sim/__init__.py
COPY sim/config.py ${LAMBDA_TASK_ROOT}/sim/config.py
COPY sim/data/ ${LAMBDA_TASK_ROOT}/sim/data/

# Copy handler
COPY lambda/handler.py ${LAMBDA_TASK_ROOT}/

CMD ["handler.lambda_handler"]
