
WORK_DIR="/home/onyxia/work"

curl https://raw.githubusercontent.com/romaintailhurat/ssphub/refs/heads/blog/polars/content/notebooks/polars-tuto.ipynb > "${WORK_DIR}/polars-tuto.ipynb"

# Open the relevant notebook when starting Jupyter Lab
jupyter server --generate-config
echo "c.LabApp.default_url = '/lab/tree/${NOTEBOOK_NAME}.ipynb'" >> /home/onyxia/.jupyter/jupyter_server_config.py
