from google.cloud import storage
from google.cloud import bigquery
from io import BytesIO
from zipfile import ZipFile
from datetime import date, datetime
import logging
import csv
import os
from libs.utils import download_ans_file, find_correct_encoding, upload_to_gcs, pad_month_num, make_ans_filename

# LOGGING ######################################################################
logger = logging.getLogger('quickstart')
log_filename = 'quickstart_' + datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '.log'
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(name)s@%(asctime)s:%(levelname)s:%(message)s')
console = logging.StreamHandler()
console.setFormatter(formatter)
logger.addHandler(console)
filehandler = logging.FileHandler(log_filename, 'a+')
################################################################################

# CONFIG #######################################################################
ftp_url = 'http://ftp.dadosabertos.ans.gov.br/FTP/PDA/informacoes_consolidadas_de_beneficiarios'

start_year = 2019

local_path = 'ans_files'

# Regiões dos arquivos
regions = [
    'AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 
    'MG', 'MS', 'MT', 'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 
    'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO', 'XX'
]

################################################################################

bqclient = bigquery.Client\
    .from_service_account_json('projeto-facul-275319-f96cc5e6816f.json')

################################################################################
os.environ["ANS_DESTINATION_BUCKET"] = "ans_files"
destination_bucket = storage_client.get_bucket(os.environ.get('ANS_DESTINATION_BUCKET'))
#destination_bucket = 'ans_files'
destination_bucket_folder = 'informacoes_consolidadas_beneficiarios'

# TODO: TRANSFORMAR EM PUBSUB
# TODO: RODAR APENAS OS ANOS, MESES E REGIOES QUE NAO ESTAO NA TABELA DE DESTINO
#  SELECT DISTINCT ARQUIVO_ORIGEM FROM projeto.dataset.tabela
#  job_result = job.result()
#  existing_files = [e[0] for e in list(job_result)]
# TODO: LIMPAR O CÓDIGO. LEIA A PEP8

existing_files = []  # remova isso depois de criar a query
end_year = date.today().year
while start_year <= end_year:
    for month in range(1, 13):
        for r in regions:
            file_to_create = make_ans_filename(start_year, month, r).replace('.csv', '_ready.csv')
            if file_to_create in existing_files:
                continue

            try:
                logger.info('iniciando download de {}-{}-{}'.format(start_year, month, r))
                made_file = download_ans_file(ftp_url, start_year, month, r, local_path)
                logger.info('download finalizado')
            except Exception:
                print(
                    'nao foi possivel baixar {} - {} - {}'
                    .format(start_year, month, r))
                continue

            destination_file = made_file.replace('.csv', '_ready.csv')
            enc = find_correct_encoding(made_file)
            arquivo_origem = made_file.split(os.sep)[-1].split('.')[0]
            header_done = False

            logger.info('iniciando adaptacao do arquivo')
            with open(made_file, newline='', encoding=enc) as sf:
                reader = csv.reader(sf, delimiter=';', quotechar='"')
                with open(destination_file, 'w+', encoding='utf-8', newline='') as df:
                    writer = csv.writer(df, dialect='unix')
                    for line in reader:
                        line.append(f"{start_year}-{pad_month_num(month)}-01")
                        writer.writerow(line)

            os.remove(made_file)
            logger.info('arquivo adaptado, original excluido')

            insert_file = f"ANS_ben{start_year}{pad_month_num(month)}_{r}_ready.csv"

            logger.info('iniciando upload')
            blob_name = '{fold}/{y}/{m}/{a}'.format(
                fold=destination_bucket_folder,
                y=start_year,
                m=month,
                a=insert_file
            )

            blob = destination_bucket.blob(blob_name)

            print(f"REALIZANDO UPLOAD DO ARQUIVO {destination_file} PARA O GCS")

            blob.upload_from_filename(destination_file)
            print(f"TÉRMINO DO UPLOAD DO ARQUIVO {destination_file} PARA O GCS")
            
            # INFO 15: Remove os arquivos que não serão mais necessários
            os.remove(destination_file)

            print(f"ARQUIVO DE DESTINO: {destination_file} DELETADO COM SUCESSO DA ÁREA TEMPORÁRIA")

            logger.info('upload finalizado')
'''
            dataset_ref = bqclient.dataset('DATASTUDIO_DEV')
            job_config = bigquery.LoadJobConfig()

            job_config.schema = [
                bigquery.SchemaField("ID_CMPT_MOVEL", "STRING"),
                bigquery.SchemaField("CD_OPERADORA", "STRING"),
                bigquery.SchemaField("NM_RAZAO_SOCIAL", "STRING"),
                bigquery.SchemaField("NR_CNPJ", "STRING"),
                bigquery.SchemaField("MODALIDADE_OPERADORA", "STRING"),
                bigquery.SchemaField("SG_UF", "STRING"),
                bigquery.SchemaField("CD_MUNICIPIO", "STRING"),
                bigquery.SchemaField("NM_MUNICIPIO", "STRING"),
                bigquery.SchemaField("TP_SEXO", "STRING"),
                bigquery.SchemaField("DE_FAIXA_ETARIA", "STRING"),
                bigquery.SchemaField("DE_FAIXA_ETARIA_REAJ", "STRING"),
                bigquery.SchemaField("CD_PLANO", "STRING"),
                bigquery.SchemaField("TP_VIGENCIA_PLANO", "STRING"),
                bigquery.SchemaField("DE_CONTRATACAO_PLANO", "STRING"),
                bigquery.SchemaField("DE_SEGMENTACAO_PLANO", "STRING"),
                bigquery.SchemaField("DE_ABRG_GEOGRAFICA_PLANO", "STRING"),
                bigquery.SchemaField("COBERTURA_ASSIST_PLAN", "STRING"),
                bigquery.SchemaField("TIPO_VINCULO", "STRING"),
                bigquery.SchemaField("QT_BENEFICIARIO_ATIVO", "STRING"),
                bigquery.SchemaField("QT_BENEFICIARIO_ADERIDO", "STRING"),
                bigquery.SchemaField("QT_BENEFICIARIO_CANCELADO", "STRING"),
                bigquery.SchemaField("DT_CARGA", "STRING"),
                bigquery.SchemaField("DT_REFERENCIA", "DATE")
            ]
            job_config.skip_leading_rows = 1
            # The source format defaults to CSV, so the line below is optional.
            job_config.source_format = bigquery.SourceFormat.CSV
            uri = f"gs://{destination_bucket}/{insert_file}"

            load_job = bqclient.load_table_from_uri(
                uri, dataset_ref.table("ANS_BENEFICIARIOS_01"), job_config=job_config
            )  # API request
            print("Starting job {}".format(load_job.job_id))

            load_job.result()  # Waits for table load to complete.
            print("Job finished.")

            destination_table = bqclient.get_table(dataset_ref.table("ANS_BENEFICIARIOS_03"))

            print("Loaded {} rows.".format(destination_table.num_rows))
'''
            #break
        #break
    #break
    #start_year += 1
print('done')
