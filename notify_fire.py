import requests
from bs4 import BeautifulSoup
import os
import boto3
import uuid

def lambda_handler(event, context):
    # URL de la página web que contiene la tabla
    url = "https://sgonorte.bomberosperu.gob.pe/24horas/?criterio=/"

    # Realizar la solicitud HTTP a la página web
    response = requests.get(url)
    if response.status_code != 200:
        return {
            'statusCode': response.status_code,
            'body': 'Error al acceder a la página web'
        }

    # Parsear el contenido HTML de la página web
    soup = BeautifulSoup(response.content, 'html.parser')

    # Encontrar la tabla en el HTML
    table = soup.find('table')
    if not table:
        return {
            'statusCode': 404,
            'body': 'No se encontró la tabla en la página web'
        }

    # Extraer los encabezados de la tabla
    headers = [header.text for header in table.find('thead').find_all('th')]
    for i in range(len(headers)):
        if headers[i] == 'Tipo':
            idx = i
            break

    # Extraer las filas de la tabla
    rows = []
    for row in table.find('tbody').find_all('tr'):  # Omitir el encabezado
        cells = row.find_all('th')
        cells.extend(row.find_all('td'))
        if len(cells) > 0 and 'INCENDIO' in cells[i].text:
            rows.append({headers[i]: cell.text for i, cell in enumerate(cells)})

    # Construir el resultado
    result = {
        'fire_cnt': len(rows),
        'fires': rows
    }

    sns_client = boto3.client('sns')
    # Publicar topico
    response = sns_client.publish(
        TopicArn=os.environ['SNS_TOPIC_ARN'],
        Message=result,
    )

    # Retornar el resultado como JSON
    return {
        'statusCode': 200,
        'body': result
    }
