# ADR-0007: Definir a retenção da bronze pelo tipo de extração, mantendo-a como fonte da verdade para reprocessar

- **Status:** aceito
- **Data:** 2026-10-19
- **Fatia:** fatia 1 — Comercial
- **Decisor:** Engenheiro de Dados Sênior (Claude) · **Acompanhamento:** usuário (pode vetar)
- **Relação:** resolve o conflito estacionado no ADR-0001 e ajusta o requisito de retenção do caso de negócio.

## Contexto

O caso de negócio pedia **bronze com 90 dias** de retenção. Já o ADR-0001 trata a bronze como fonte da verdade para reprocessamento, e o requisito de idempotência diz que "qualquer dia pode ser reprocessado". As duas regras se contradizem.

O ERP entrega dois tipos de extração:
- **incremental**, para os fatos: cada pedido ou NF chega uma vez, mais as atualizações;
- **full**, para os cadastros: a cada dia chega a tabela inteira, com todo o histórico de versões (ADR-0006).

## Decisão

**A retenção depende de a linha existir em outro lugar ou não.**

| O quê | Retenção | Por quê |
|---|---|---|
| Bronze de dataset **incremental** (pedidos, NFs) | Indefinida, tudo | É a única cópia: apagar tira a capacidade de reconstruir a silver e a gold |
| Bronze de dataset **full** (cliente, produto, hierarquia) | As últimas 90 extrações | Cada extração já contém o histórico inteiro, então as antigas são redundantes |
| Arquivos na landing (todas as fontes) | 90 dias | Servem de margem operacional para reprocessar a bronze em caso de bug na ingestão |

**Implementação:**
- A limpeza dos datasets full entra como tarefa do job quando a bronze passar de 90 extrações. Até lá, ela seria código sem uso (YAGNI).
- O direito ao esquecimento (LGPD) se resolve apagando o titular da bronze, em todas as extrações, e da landing, seguido de `VACUUM`. Silver e gold não guardam PII (ADR-0004).

## Alternativas consideradas

| Alternativa | Por que não |
|---|---|
| 90 dias para toda a bronze (requisito original) | Depois de 90 dias, um *full refresh* da silver perderia os fatos antigos para sempre, e a idempotência só valeria para a janela recente |
| Guardar tudo para sempre | As extrações full diárias de ~70 mil versões de cliente somariam ~25 mi de linhas por ano sem nenhum valor |
| Usar a silver como fonte da verdade | A silver já aplica regras (dedup, limpeza). Um bug numa regra corrompe a "verdade" sem caminho de volta ao dado cru |

## Consequências

- **Positivas:**
  - a idempotência vale para qualquer data;
  - o armazenamento cresce só com o que é único (os fatos, pequenos: ~3 mi de itens em 18 meses);
  - o esquecimento de um titular tem um procedimento claro.
- **Negativas / dívidas assumidas:**
  - a bronze de fatos cresce sem limite;
  - a rotina de limpeza dos datasets full ainda vai ser escrita;
  - apagar um titular exige percorrer todas as extrações da bronze.
- **O que nos faria revisitar:**
  - o custo de armazenamento da bronze de fatos passar a ser relevante;
  - surgir uma exigência legal de prazo máximo de guarda.

## Em produção faríamos diferente?

- **Prazo:** a retenção indefinida vira o prazo legal. O de documentos fiscais costuma ser de 5 anos (decadência tributária), e muitas empresas adotam 7 por margem. A política é definida com o jurídico e o fiscal.
- **Armazenamento:** dados antigos iriam para um armazenamento mais barato.
- **Time travel:** a retenção de time travel e de `VACUUM` seria configurada de forma explícita.

**Em 10x e 100x de volume:** a regra se mantém, e o custo cresce linearmente só nos fatos.

## Conceitos para estudar

- bronze imutável como fonte da verdade;
- extração full vs incremental e seus efeitos na retenção;
- *full refresh* e reconstrução;
- direito ao esquecimento num lakehouse (`DELETE` + `VACUUM`, time travel);
- prazos de guarda fiscal.
