# **Objetivo**  

OpenSearch (OS)  

Criar um script que avalie para cada tamanho de segmentação do texto:
- O tempo de execução para realizar uma busca geral;
- O tempo de execução para realizar buscas específicas (com termo genérico e específico);
- A quantidade de documentos retornados;
- O tamanho do índice;

# **Caminho**
<!-- - Parar o container do OS
- Reiniciar o container do OS
- Criar uma variável de ambiente para o tamanho do segmento
- Criar uma variável de ambiente para usar ou não o recurso de highlight 
- Chamar a função para gerar os segmentos e indexar no OS -->
<!-- - Obter tamanho do índice (comando p/ OS)
- Obter qtd de documento (comando p/ OS) -->
- Realizar busca geral e obter o tempo de execução (comando p/ OS)
- Realizar busca específica e obter o tempo de execução (comando p/ OS)
- Parar o container do OS

# **Modificações**
- Atualização do scikit-learn 1.0.2 >>> 1.5.2

# **Sugestao**
Uma sugestão pra fazer ele ficar todo automático pra todas as variações seria desvincular o SEGMENT_SIZE do envvars e do ambiente em que o script tá sendo executado.

Inicializa SEGMENT_SIZES como uma lista com todas as variações. Itera sobre essa lista. Pra cada segment_size, faz o que tu tava fazendo antes com SEGMENT_SIZE. Pra fazer funcionar no container, continua usando no podman run o ```--env-file envvars``` mas também adiciona o argumento ```-e SEGMENT_SIZE={segment_size}``` (assumindo uso de string formatada aqui) que vai entrar como uma variável de ambiente prioritária (cria ou substitui o valor se ela já existir no envvars). 

# **Resolucao**
- Criar uma funcao dentro de gazette_text para checar se deve segmentar o texto ou nao
- Separar o processo de segmentacao em outra funcao
- Passar SEGMENT_SIZE como uma variavel de ambiente prioritaria
- 









