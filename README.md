# Python Sequential Kafka & API

Este repositório é responsável por enviar mensagens para tópicos Kafka e apis. Para utilizar este script, basta inserir as informações necessárias no arquivo CSV e seguir os passos abaixo para configurar o ambiente e executar o envio das mensagens para o tópico.

## Montagem do Ambiente para Executar o script.py

Para garantir que as dependências do projeto não entrem em conflito com outras bibliotecas Python instaladas globalmente, crie e ative um ambiente virtual:

`````
python3 -m venv venv

source venv/bin/activate
`````

## **Instale as Dependências**

Com o ambiente virtual ativado, instale as dependências necessárias para executar o script:

`````
pip install confluent-kafka
`````

### **Configure o Kafka**

Antes de executar o script, certifique-se de que você tem as credenciais e o acesso necessário ao cluster Kafka.

-   Verifique se as variáveis `KAFKA_SERVERS`, `sasl.username`, e `sasl.password` estão configuradas corretamente no arquivo `.py`.

### **Execute o Script**

Após configurar tudo, você pode executar o script com o comando:

`python3 script.py`

### **Desativar o Ambiente Virtual**

Quando terminar, você pode desativar o ambiente virtual com o comando:

`deactivate`