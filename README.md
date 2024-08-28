# MapReduce

## Contexto:

Você foi contratado para integrar a equipe de análise de dados de uma grande empresa
multinacional. Nessa equipe, é utilizada a tecnologia Hadoop para processar grandes bases de dados
utilizando a linguagem de programação Java. No projeto atual, você deverá utilizar o modelo de
programação MapReduce para extrair uma série de informações sobre transações comerciais
internacionais realizadas pela empresa nos últimos 30 anos. Essas transações estão armazenadas em um
dataset estruturado com 10 colunas, conforme a descrição apresentada na tabela abaixo:

*===================*===============================================================*
| Variable (column) | Description                                                   |
*===================*===============================================================*
| Country           | Country involved in the commercial transaction                |
*-------------------*---------------------------------------------------------------*
| Year              | Year in which the transaction took place                      |
*-------------------*---------------------------------------------------------------*
| Commodity code    | Commodity identifier                                          |
*-------------------*---------------------------------------------------------------*
| Commodity         | Commodity description                                         |
*-------------------*---------------------------------------------------------------*
| Flow              | Flow, e.g. Export or Import                                   |
*-------------------*---------------------------------------------------------------*
| Price             | Price, in USD                                                 |
*-------------------*---------------------------------------------------------------*
| Weight            | Commodity weight                                              |
*-------------------*---------------------------------------------------------------*
| Unit              | Unit in which the commodity is measured, e.g. Number of items |
*-------------------*---------------------------------------------------------------*
| Amount            | Commodity amount given in the aforementioned unit             |
*-------------------*---------------------------------------------------------------*
| Category          | Commodity category, e.g. Live animals                         |
*===================*===============================================================*

O dataset contêm mais 8 milhões de exemplos (ou seja, 8 milhões de linhas que representam as
transações comerciais da empresa). Esse dataset é disponibilizado no formato CSV em que as colunas são
separadas por ponto e vírgula “;”. Na imagem abaixo, são apresentadas as primeiras 5 linhas do arquivo,
cada uma com um total de 10 colunas.

## Objetivos:

De acordo com o contexto apresentado acima, você e sua equipe são responsáveis por
desenvolver soluções em MapReduce capazes de responder as seguintes perguntas:

1. (1,0 ponto) Número de transações envolvendo o Brasil.
2. (1,0 ponto) Número de transações por ano.
3. (1,0 ponto) Número de transações por categoria.
4. (1,0 ponto) Número de transações por tipo de fluxo (flow).
5. (1,5 ponto) Valor médio das transações por ano somente no Brasil.
6. (1,5 ponto) Transação mais cara e mais barata no Brasil em 2016.
7. (1,5 ponto) Valor médio das transações por ano, considerando somente as transações do
   tipo exportação (Export) realizadas no Brasil.
8. (2,0 ponto) Transação com o maior e menor preço comercializada em 2016 (com base na
   coluna amount), por ano e país.

## Regras:

Para cada um dos itens acima, forneça:

1. Será necessário retirar o cabeçalho.
2. Será necessário tratar dados faltantes.
3. Código fonte para a resolução do problema utilizando MapReduce em Java. ATENÇÃO: não
   serão consideradas como corretas, soluções que realizam a concatenação de strings para a
   formação de chaves ou valores compostos.
4. O resultado da execução em um arquivo separado e no formato txt.

## Arquivo utilizado:

Link do arquivo csv com os dados manipulados: https://drive.google.com/file/d/1N0xJ8YnlsS8BhvtiFc_KrotglyFrelFV/view?usp=sharing