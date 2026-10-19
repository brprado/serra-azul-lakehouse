# Guia do Databricks AI Dev Kit

> Guia prático para desenvolver projetos de engenharia de dados no Databricks com o Claude Code e o AI Dev Kit.
> Instalador: <https://github.com/databricks-solutions/ai-dev-kit> · Fonte das skills: <https://github.com/databricks/databricks-agent-skills>
> Ambiente de teste: Databricks **Free Edition** (AWS), perfil de CLI `aws`.
> Montado em 2026-10-15 com CLI v1.21.0 e skills `databricks-agent-skills` v0.2.30.

---

## 1. O que é o AI Dev Kit (em uma frase)

É um **pacote de conhecimento + ferramentas** que ensina o seu assistente de IA (Claude Code, Cursor, Codex, Gemini CLI etc.) a trabalhar no Databricks do jeito certo: sabe quais comandos da CLI usar, quais APIs são as atuais, quais armadilhas evitar e qual a estrutura recomendada de um projeto.

Sem o kit, o assistente "chuta" a sintaxe (ainda usa `import dlt`, `CREATE LIVE TABLE`, `LIVE.` etc.). Com o kit, ele consulta uma **skill** antes de agir e segue as práticas atuais.

### As 4 peças do repositório

| Peça | O que é | Precisa para o nosso caso? |
|---|---|---|
| **Skills** | Arquivos `SKILL.md` + referências que o assistente lê sob demanda. Instaladas e mantidas pela CLI (`databricks aitools`). | **Sim, é o núcleo.** Já instalado. |
| **MCP server** (`databricks-mcp-server/`) | Expõe 40+ ferramentas Databricks para clientes de IA via protocolo MCP. Instalação independente, mantido em *best-effort*. | Opcional. A CLI já cobre o dia a dia. |
| **Core library** (`databricks-tools-core/`) | Biblioteca Python com funções de alto nível (ex.: `execute_sql`). | Opcional. Útil se quisermos montar automações próprias. |
| **Visual Builder App** (`databricks-builder-app/`) | Aplicação web com chat + Claude Code, para rodar local ou fazer deploy como Databricks App. | Opcional. Interface visual alternativa. |

### Quem é quem: dois repositórios, dois papéis

| Repositório | Papel hoje |
|---|---|
| **[`databricks/databricks-agent-skills`](https://github.com/databricks/databricks-agent-skills)** | **Fonte oficial do conteúdo das skills** (`SKILL.md` e `references/`). É mantido pela Databricks e distribuído pela CLI com `databricks aitools install`. Foi a release 0.2.30 dele que instalamos. |
| **[`databricks-solutions/ai-dev-kit`](https://github.com/databricks-solutions/ai-dev-kit)** | **Instalador e extras.** O `install.sh` não contém skills: ele chama `databricks aitools install`, acrescenta as skills de MLflow (do repo `mlflow/skills`), escolhe o perfil de skills (`data-engineer`, `analyst`...), configura vários assistentes de IA de uma vez e remove instalações antigas. O repo também traz o MCP server, a core library e o Builder App. |

Ou seja: **o ai-dev-kit continua sendo o ponto de entrada recomendado**, mas as skills em si vêm do `databricks-agent-skills`. As mesmas skills podem ser instaladas só com a CLI, sem o ai-dev-kit:

```bash
databricks aitools install --scope project --agents claude-code --skills-only \
        --skills databricks-pipelines,databricks-jobs,databricks-dabs,databricks-core
```

O que o ai-dev-kit acrescenta sobre isso é a conveniência (perfis prontos, detecção e atualização) e as skills de MLflow.

> Observação importante: as skills deixaram de morar no repositório ai-dev-kit; os arquivos que sobraram lá estão obsoletos e servem só de referência. Nomes antigos foram trocados: `databricks-bundles` virou `databricks-dabs`, `databricks-spark-declarative-pipelines` virou `databricks-pipelines`, `databricks-config` virou `databricks-core`. Se encontrar tutoriais antigos com os nomes velhos, é por isso. Para o layout antigo, o ai-dev-kit mantém a tag `v0.1.14`.

---

## 2. O que foi instalado nesta máquina

| Item | Valor |
|---|---|
| Escopo | **Projeto** (só vale ao abrir o Claude Code nesta pasta) |
| Pasta do projeto | `~/Documents/DataEngineering Projects/Databricks Project (AI Dev Kit test)` |
| Perfil Databricks | `aws` (Free Edition, `dbc-6dc80a99-8c95.cloud.databricks.com`) |
| Perfil de skills | `data-engineer` (15 skills: 4 core + 11 de engenharia de dados) |
| Skills em | `.claude/skills/` |
| Estado da instalação | `.ai-dev-kit/skills.lock` e `.databricks/aitools/` |
| Perfil fixado | `.claude/settings.json` define `DATABRICKS_CONFIG_PROFILE=aws` |

**Por que fixamos o perfil `aws`?** A máquina tem dois perfis: `empresa` (Azure, marcado como padrão) e `aws` (Free Edition). Sem a variável, qualquer comando sem `--profile` iria para o workspace da empresa. A variável protege o projeto de testes. Para voltar ao comportamento anterior, apague `.claude/settings.json`.

### Como o assistente "usa" as skills

1. Ao abrir o Claude Code **nesta pasta**, ele lê o `name` e a `description` de cada skill em `.claude/skills/`.
2. Quando o seu pedido combina com uma descrição (ex.: "criar pipeline com CDC"), ele carrega o `SKILL.md` correspondente.
3. O `SKILL.md` aponta para arquivos em `references/` que são lidos só se a tarefa exigir (economiza contexto).
4. Você também pode forçar uma skill digitando `/databricks-pipelines`, `/databricks-jobs` etc.

> Se uma skill instalada não aparecer, reinicie o Claude Code dentro da pasta do projeto.

### Gerenciar as skills

```bash
databricks aitools list                      # o que existe / o que está instalado
databricks aitools update                    # atualizar para a versão mais nova
databricks aitools install --scope project --agents claude-code --skills-only \
        --skills databricks-serverless,databricks-data-discovery   # adicionar skills
databricks aitools uninstall                 # remover
```

Reinstalar ou atualizar tudo também funciona rodando de novo o instalador do ai-dev-kit (ele detecta a instalação existente e atualiza no lugar). Use `--dry-run` para ver o plano sem alterar nada e `--list-skills` para ver os perfis.

---

## 3. Catálogo das skills instaladas

### 3.1 Núcleo (sempre instaladas)

| Skill | Para que serve | Exemplo de pedido |
|---|---|---|
| **databricks-core** | Porta de entrada: instalação da CLI, autenticação, **seleção de perfil**, comandos básicos, bundles, exploração de dados (`aitools tools query`). Roteia para as outras skills. | "Liste os catálogos e schemas do meu workspace." |
| **databricks-docs** | Consulta a documentação oficial via índice `llms.txt`. Fallback quando nenhuma outra skill cobre o tema. | "Como configuro X? Ache na documentação oficial." |
| **databricks-python-sdk** | `databricks-sdk`, Databricks Connect, REST. API de clusters, jobs, SQL, UC, volumes, secrets, dbutils. | "Escreva um script Python que lista os jobs e o último status de cada um." |
| **databricks-unity-catalog** | Governança: criar catálogos/schemas/volumes, `GRANT`/`REVOKE`, row filters, column masks, **system tables** (auditoria, linhagem, billing), arquivos em `/Volumes/`. | "Quem tem acesso à tabela X?" / "Crie um volume para a landing zone." |

### 3.2 Engenharia de dados (perfil `data-engineer`)

| Skill | Para que serve | Exemplo de pedido |
|---|---|---|
| **databricks-pipelines** | **Lakeflow Spark Declarative Pipelines** (antigo Delta Live Tables), em Python ou SQL. Streaming tables, materialized views, Auto Loader, **Auto CDC (SCD 1 e 2)**, expectations (qualidade), sinks (Kafka, Delta), Real-Time Mode. Contém árvore de decisão e tabela de migração de DLT legado. | "Crie um pipeline bronze/silver/gold lendo CSVs de um volume, com expectations e SCD2 no cadastro de clientes." |
| **databricks-jobs** | **Lakeflow Jobs**: DAGs multi-tarefa, tipos de task (notebook, python, wheel, SQL, dbt, pipeline, for_each, run_job), triggers (cron, periódico, chegada de arquivo, atualização de tabela, contínuo), retries, notificações, compute serverless. | "Monte um job diário às 6h que roda o pipeline e depois uma task SQL de validação, com alerta em falha." |
| **databricks-dabs** | **Declarative Automation Bundles** (antigo Asset Bundles): `databricks.yml`, multi-ambiente (dev/prod), variáveis, deploy, permissões. É o que transforma tudo em **código versionado**. | "Empacote o job e o pipeline num bundle com targets dev e prod." |
| **databricks-spark-structured-streaming** | Spark Structured Streaming "na unha": triggers, watermarks, joins stream-stream, checkpoints, múltiplos sinks, Real-Time Mode, tuning de custo. | "Qual trigger usar para processar o backlog uma vez e parar (`availableNow`)?" |
| **databricks-dbsql** | Recursos avançados de SQL no warehouse: SQL scripting, stored procedures, pipe syntax (`|>`), AI Functions (`ai_query`, `ai_classify`...), `read_files`, `remote_query` (federação), recursive CTE, H3. | "Crie uma stored procedure de carga com tratamento de erro." |
| **databricks-iceberg** | Tabelas Iceberg gerenciadas, leitura Iceberg de tabelas Delta (UniForm), Iceberg REST Catalog, interoperabilidade (Snowflake, PyIceberg, Spark OSS). | "Exponha minha tabela Delta para leitura por outro engine via Iceberg." |
| **databricks-lakeflow-connect** | Ingestão **gerenciada** de SaaS (Salesforce, Workday, ServiceNow, GA4, HubSpot...) e bancos (SQL Server, PostgreSQL/MySQL em preview). | "Ingerir contas e oportunidades do Salesforce." (requer a fonte; ver limites do Free Edition) |
| **databricks-zerobus-ingest** | Enviar eventos direto para tabelas Delta sem broker (SDKs, REST, OTLP, MQTT, Kafka-compatível). | "Quero que um app envie eventos de telemetria direto para uma tabela." |
| **spark-python-data-source** | Criar *data sources* customizados em Python (leitor/escritor batch e streaming) para sistemas sem conector nativo. Experimental. | "Leia dados desta API REST como se fosse uma tabela Spark." |
| **databricks-metric-views** | **Metric Views** do Unity Catalog: métricas de negócio (KPIs) definidas uma vez em YAML e reutilizadas por dashboards, Genie e SQL. | "Defina receita, ticket médio e pedidos como métricas governadas." |
| **databricks-synthetic-data-gen** | Gera dados sintéticos realistas com Spark + Faker (distribuições ponderadas, log-normal, integridade referencial). Pergunta antes qual catálogo usar. | "Gere 1 milhão de pedidos fictícios para testar o pipeline." |

### 3.3 Skills disponíveis que **não** instalamos (pode adicionar quando precisar)

| Área | Skills |
|---|---|
| Dados e compute | `databricks-data-discovery` (perguntas em linguagem natural, Genie), `databricks-serverless` (escolha de compute serverless vs. clássico), `databricks-execution-compute`, `databricks-serverless-migration` |
| BI / analytics | `databricks-aibi-dashboards`, `databricks-genie-agents` |
| Apps | `databricks-apps`, `databricks-apps-python`, `databricks-app-design`, `databricks-lakebase` (Postgres gerenciado) |
| IA / ML | `databricks-agent-bricks`, `databricks-ai-functions`, `databricks-ai-runtime`, `databricks-model-serving`, `databricks-vector-search`, `databricks-ml-training`, `databricks-mlflow-evaluation`, `databricks-unstructured-pdf-generation`, `databricks-unity-gateway` |
| Ambiente | `databricks-setup-local` |
| MLflow (repo `mlflow/skills`) | `agent-evaluation`, `analyze-mlflow-trace`, `instrumenting-with-mlflow-tracing`, `querying-mlflow-metrics`, e outras |

Sugestão para o nosso projeto: `databricks-serverless` (o Free Edition só tem compute serverless) e `databricks-data-discovery`; `databricks-aibi-dashboards` quando formos criar a camada de consumo.

---

## 4. Como tirar proveito: o fluxo de um projeto

Esse é o caminho recomendado pelas próprias skills para um projeto de dados de ponta a ponta.

```
 1. Dados de teste   → databricks-synthetic-data-gen
 2. Governança       → databricks-unity-catalog   (schema, volume, permissões)
 3. Pipeline         → databricks-pipelines       (bronze → silver → gold)
 4. Orquestração     → databricks-jobs            (schedule, tasks, alertas)
 5. Empacotamento    → databricks-dabs            (dev/prod, deploy via CLI)
 6. Métricas e BI    → databricks-metric-views (+ dashboards)
```

### Boas práticas que as skills já embutem (e que valem para qualquer projeto)

- **Medalhão** (Bronze → Silver → Gold) por padrão, começando simples (poucas tabelas).
- **Streaming Table** para fonte incremental e **Materialized View** para agregações; não se muda um tipo para o outro depois (precisa dropar).
- **Intermediários como `TEMPORARY VIEW`**, e união de streams com **Append Flows** (nunca `UNION`).
- **Gold preserva as dimensões** que os analistas vão filtrar (local, produto, período...). Agregar demais perde informação irrecuperável.
- **Liquid Clustering** (`CLUSTER BY`) no lugar de `PARTITIONED BY` + `ZORDER`.
- **Expectations** (`expect`, `expect_or_drop`, `expect_or_fail`) para qualidade de dados desde a camada bronze/silver.
- **Todo deploy via bundle** (`databricks bundle validate` → `deploy` → `run`), com catálogo/schema/warehouse como **variáveis** para alternar dev/prod.
- **Em desenvolvimento use `mode: development`** no bundle: os recursos ganham prefixo com seu usuário e agendamentos nascem pausados.
- **Pipelines: selective refresh** quando possível. **Full refresh é perigoso** (reprocessa o streaming do zero e destrói estado); a skill instrui o assistente a só sugerir e esperar sua aprovação.
- **Poll do update, não do estado do pipeline**: o estado de topo não revela a causa real da falha; o erro está em `error.exceptions[0].message` nos eventos.

### Três formas de iniciar um pipeline (a skill pergunta qual)

| Workflow | Quando | Comandos-chave |
|---|---|---|
| **A**: projeto novo em bundle | Produção, projeto "dono" do pipeline | `databricks pipelines init` |
| **B**: pipeline num bundle existente | Já existe `databricks.yml` | adicionar `resources/<nome>.pipeline.yml` |
| **C**: iteração rápida pela CLI (sem bundle) | Exploração, protótipo | `databricks pipelines create` / `start-update` / `list-pipeline-events` |

---

## 5. Cola rápida: comandos da CLI

Sempre passe `--profile aws` (ou confie na variável fixada neste projeto).

```bash
# Diagnóstico
databricks auth profiles
databricks current-user me --profile aws
databricks aitools list

# Unity Catalog (argumentos POSICIONAIS, não flags)
databricks catalogs list --profile aws
databricks schemas list workspace --profile aws
databricks tables list workspace <schema> --profile aws

# SQL e exploração (comandos feitos para agentes)
databricks experimental aitools tools get-default-warehouse --profile aws
databricks experimental aitools tools query "SELECT * FROM samples.nyctaxi.trips LIMIT 10" --profile aws
databricks experimental aitools tools discover-schema samples.nyctaxi.trips --profile aws

# Recursos
databricks jobs list --profile aws
databricks pipelines list --profile aws
databricks warehouses list --profile aws

# Bundles (o coração do fluxo)
databricks bundle init default-python --profile aws
databricks bundle validate --strict -t dev --profile aws
databricks bundle deploy -t dev --profile aws
databricks bundle run <recurso> -t dev --profile aws

# Volumes (note o prefixo dbfs: mesmo para /Volumes)
databricks fs ls dbfs:/Volumes/<catalogo>/<schema>/<volume>/
```

Armadilhas de CLI que a skill `databricks-core` avisa:

- `databricks schemas list --catalog-name X` **não existe**. Use posicional: `schemas list X`.
- `databricks sql execute` e `execute-statement` **não existem**. Use `experimental aitools tools query`.
- Cada comando do Claude Code roda em um **shell separado**: `export VAR=...` numa chamada não vale na próxima. Use `--profile` ou encadeie com `&&`.
- Nomes são literais: não troque hífen por underscore. Em SQL, use crases para nomes com caractere especial.

---

## 6. Regras de segurança e custo

1. **Perfil**: a skill `databricks-core` manda o assistente **nunca escolher perfil sozinho**; ele deve listar e perguntar. Responda sempre `aws` neste projeto. O perfil `empresa` não deve ser usado aqui.
2. **O instalador executa código remoto** (`curl | bash`). Neste setup baixamos o script, lemos e rodamos `--dry-run` antes. Faça igual nas atualizações.
3. **Operações destrutivas** (full refresh, `bundle destroy`, `DROP`, `REVOKE`) merecem confirmação explícita sua.
4. **O escopo "projeto" é preso à pasta**: abrir o Claude Code em outro diretório não carrega as skills. Para ter em todo lugar, reinstale com `--global`.

---

## 7. Free Edition: o que muda na prática

O Free Edition é ótimo para aprender, mas tem restrições. Valide as atuais na documentação oficial do Databricks (página "Free Edition limitations"), pois mudam com o tempo. O que observamos nesta conta:

| Observado | Consequência |
|---|---|
| `databricks clusters list` retorna vazio | Não há compute clássico: use **serverless** (jobs, pipelines, notebooks). Nos exemplos das skills que usam `new_cluster` / `node_type_id`, troque por serverless. |
| Catálogo `workspace` (gerenciado) + `samples` + `system` | Use `workspace` como catálogo nos bundles (o template `default-python` já vem assim). |
| Um único warehouse: *Serverless Starter Warehouse* | Serve para SQL, tarefas SQL de jobs e `aitools tools query`. |
| Provavelmente sem conectores SaaS gerenciados | `databricks-lakeflow-connect` pode não ser viável aqui; tratar como estudo. |

---

## 8. Resultado dos primeiros testes (o que funcionou e o que não)

| Teste | Resultado |
|---|---|
| Instalação das skills (perfil `data-engineer`) | OK, 15/35 skills instaladas, `aitools list` mostra tudo "up to date" |
| `current-user me` no perfil `aws` | OK (conta Free Edition) |
| `catalogs list`, `warehouses list`, `workspace list` | OK |
| `bundle init default-python` (job + pipeline + serverless) | OK, gerou projeto completo |
| `bundle validate` no projeto gerado | **OK** (`Validation OK!`, target `dev`, host do Free Edition) |
| `aitools tools query` (SQL real) | **Falhou**: `start warehouse 773a509ca8bf0013: Cannot create the resource, please try again later.` Reproduzido em 4 tentativas, inclusive com `databricks warehouses start`. |

**Sobre a falha do warehouse** *(resolvido depois)*: não era o kit nem o tamanho do warehouse — era a ***fair usage quota*** do Free Edition. A documentação oficial (*Free Edition limitations*) confirma: estourar a cota **desliga o compute do workspace pelo resto do dia** (em casos extremos, do mês). Na mesma conta, `jobs submit` também falhou com "Triggering new runs is currently disabled temporarily", o que bate com o bloqueio geral de compute. Em outra conta Free Edition (`aws2`), o mesmo notebook rodou normalmente em serverless. Moral: se tudo parar de ligar de uma vez, provavelmente é cota — espere o reset diário.

---

## 9. Roteiro sugerido para o primeiro projeto

Uma sequência didática, do simples ao completo. Em cada passo, o prompt já ativa a skill correta.

1. **Reconhecimento**: "Liste meus catálogos, schemas e o que existe no workspace `aws`."
2. **Dados de teste**: "Gere dados sintéticos de e-commerce (clientes, produtos, pedidos) em um volume do catálogo `workspace`." *(synthetic-data-gen + unity-catalog)*
3. **Pipeline**: "Crie um pipeline SDP em SQL: bronze com Auto Loader, silver com expectations e SCD2 de clientes, gold de vendas por dia/categoria." *(pipelines)*
4. **Orquestração**: "Crie um job diário que executa o pipeline e depois uma validação SQL, com notificação de falha." *(jobs)*
5. **Empacotamento**: "Organize tudo em um bundle com targets `dev` e `prod` e faça o deploy em `dev`." *(dabs)*
6. **Consumo**: "Defina métricas de negócio como metric view." *(metric-views)*
7. **Evolução**: instalar `databricks-aibi-dashboards` para o dashboard e `databricks-data-discovery` para perguntas em linguagem natural.

**Dica de ouro**: peça sempre "mostre o plano antes de executar". As skills de dados (ex.: synthetic-data-gen e zerobus) já fazem isso por padrão, e você mantém o controle sobre custo e escopo.

---

## 10. Referências

- Instalador e extras (MCP, core lib, Builder App): <https://github.com/databricks-solutions/ai-dev-kit>
- **Fonte oficial das skills**: <https://github.com/databricks/databricks-agent-skills>
- Skills de MLflow: <https://github.com/mlflow/skills>
- Exemplos de bundles: <https://github.com/databricks/bundle-examples>
- Documentação DABs: <https://docs.databricks.com/dev-tools/bundles/>
- Pasta local com as skills (leitura recomendada): `.claude/skills/<skill>/SKILL.md` e `references/`
