---
name: anki
description: >-
  Estudos do Bruno com Anki (repetição espaçada): cria flashcards a partir de um assunto, de uma nota do vault, de uma aula ou de texto colado, guarda os cards versionados em notas de deck no vault (03 Resources/Anki) e os envia ao Anki pelo MCP do AnkiMCP Server; sincroniza edições, conduz revisões pelo chat, mostra estatísticas (devidos hoje, acerto, previsão, cards difíceis) e registra as sessões e o balanço na nota "Estudos com Anki". Use sempre que ele falar de Anki, flashcards, cards, deck, repetição espaçada, "cria cards disso", "transforma essa nota em cards", "quero memorizar", "o que tenho para revisar", "me pergunta os cards" ou "como estão meus estudos", mesmo sem dizer "Anki".
---

# /anki — estudos com Anki versionados no vault

## Uso a partir do projeto Serra Azul

Cópia da skill do vault (`/Users/brunoprado/Documents/SecondBrain/.claude/skills/anki`), para criar cards com o conteúdo deste projeto. O que muda ao usá-la daqui:

- **Os cards continuam no vault**, que é a fonte única deles. Todo caminho relativo desta skill (`02 Areas/…`, `03 Resources/Anki/…`, `99 System/…`) é relativo a `/Users/brunoprado/Documents/SecondBrain`. Os scripts rodam a partir de lá: `cd /Users/brunoprado/Documents/SecondBrain && python3 -I -B "99 System/Integrações/anki/anki_cards.py" <comando>`.
- **O material vem deste projeto:** ADRs (`docs/adr/`), convenções, modelagem, código dos pipelines e resultados das execuções. Use os exemplos do Serra Azul e cite o ADR de origem na nota de deck (linha "Origem:"). SQL em **Databricks SQL**, o dialeto do projeto, e não em T-SQL.
- **Sem MCP do Anki aqui:** o servidor `anki` está no `.mcp.json` do vault, não no deste projeto. Use o cliente `99 System/Integrações/anki/anki_mcp.py` (seção 0, item 2), rodando a partir do vault.
- **Commit (seção 7):** é feito no repositório do vault, que usa a conta pessoal `brprado`. Neste projeto, commit só quando o usuário pedir (CLAUDE.md).

Responda sempre em português. Antes de qualquer ação, leia **`02 Areas/Pessoal/Estudos com Anki.md`** (o painel e a memória desta skill) e atualize-o depois.

**Divisão de responsabilidades:**

- O **vault** guarda o conteúdo dos cards, em notas de deck `03 Resources/Anki/Cards — <Tema>.md`, que vão para o git. É a fonte da verdade do texto.
- O **Anki** guarda o agendamento, o histórico de revisões e as estatísticas. Nunca tente reproduzir isso no vault, só registre resumos.
- O **script** `99 System/Integrações/anki/anki_cards.py` faz a parte determinística: lê as notas de deck, diz o que é novo ou editado, converte markdown em HTML e grava o ID do Anki em cada card. Rode sempre da raiz do vault com `python3 -I -B "99 System/Integrações/anki/anki_cards.py" <comando>`.
- O **MCP** (`mcp__anki__*`, servidor `anki` do `.mcp.json`) é o único canal com o Anki. Confira o schema da ferramenta antes de chamá-la: os nomes de parâmetro vêm de lá, não desta skill.

## 0. Conexão

1. Se as ferramentas `mcp__anki__*` não aparecerem, ou falharem por conexão, diga o motivo provável e pare:
   - o **Anki está fechado** (o servidor só existe com o app aberto);
   - o add-on **AnkiMCP Server** não está instalado (Anki → Ferramentas → Complementos → Obter complementos → código `124672614`, depois reiniciar o Anki);
   - o Claude Code precisa ser reiniciado ou o servidor `anki` do `.mcp.json` precisa ser aprovado (`/mcp`).
2. **Ferramentas ausentes, mas o Anki aberto:** se `curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:3141/` devolver um código HTTP (406 é o normal), o servidor está no ar e só a sessão não carregou as ferramentas. Use o cliente `99 System/Integrações/anki/anki_mcp.py` (`tools`, `call <tool> '<json>'`, `resource <uri>`), que fala o mesmo protocolo, em vez de pedir reinício. Para lotes, passe os argumentos pela entrada padrão (`call add_notes -`).
3. **Não use AnkiConnect** nem a porta 8765: ela é do painel do DataOps Triage. O AnkiMCP usa a 3141.
4. Mesmo sem conexão você pode escrever cards no vault (seção 2); eles ficam como "novos" e sobem no próximo envio.

### Primeira conexão

Se `## Configuração detectada` do painel ainda estiver vazia:

1. Leia o recurso `anki://system-info` (versão, perfil) e liste `model_names` e `model_field_names` do tipo básico. Em português costuma ser **Básico** (`Frente`, `Verso`); em inglês, **Basic** (`Front`, `Back`). Use os nomes reais.
2. Grave no painel, em lista curta: versão do Anki, perfil, tipo básico e campos, e se ele usa AnkiWeb (pergunte uma vez; isso decide se você oferece `sync` ao fim dos envios).

## 1. Escrever bons cards

Siga as regras de formulação (o MCP tem o prompt `twenty_rules`, baseado nas 20 regras da SuperMemo):

- **Só pergunta e resposta (tipo básico). Nunca use lacuna** (`{{c1::…}}`, tipo Cloze/Omissão): ela esconde palavras da frase com `[...]`, e o Bruno não quer esse formato (pedido de 27/10/2026). Definição, sintaxe ou termo viram pergunta direta: "o que é X?", "qual comando faz Y?", "como é o formato de Z?".
- **Um fato por card.** Pergunta curta e resposta curta (idealmente uma frase ou um termo). Quebre listas em vários cards.
- Entenda antes de decorar: o card cobre o que já foi estudado, não substitui a nota.
- Pergunta sem ambiguidade, com contexto mínimo no início se preciso (`Spark:`, `T-SQL:`, `Airflow:`).
- A frente é sempre uma **pergunta** que termina em `?`, nunca uma frase afirmativa para completar.
- Evite cards de "liste todos os X". Evite respostas sim/não.
- Código curto em `` ` `` ou bloco de código. Exemplos concretos do trabalho dele (Airflow, Databricks, T-SQL, Protheus) fixam melhor.
- SQL em **T-SQL** (o dialeto dele, como na skill `/leetcode`).
- Quantidade: 5 a 15 cards por nota ou aula. Mais que isso, pergunte antes.

## 2. Criar cards

Pedidos como "cria cards de X", "transforma a nota Y em cards", "cards da aula 2 do bootcamp", ou texto colado.

1. Se a fonte é uma nota do vault, leia-a. Se é um assunto solto, escreva a partir do que você sabe com segurança; em dúvida sobre um fato, deixe-o de fora ou avise.
2. **Escolha o deck.** Veja a tabela `## Decks` do painel. Reaproveite um deck existente quando o assunto couber; senão, proponha um novo no formato hierárquico `Área::Tema` em Title Case português (ex.: `Engenharia de Dados::Spark`, `Engenharia de Dados::Airflow`, `SQL::T-SQL`, `Idiomas::Inglês`), com a nota `03 Resources/Anki/Cards — <Tema>.md`. **O `create_deck` aceita no máximo 2 níveis** (`Pai::Filho`): para subtemas, use notas de deck separadas apontando para o mesmo deck (a tag `sb::<nota>` distingue os subtemas no Anki).
3. **Nota de deck nova:** crie com o texto final (sem sintaxe do Templater), seguindo `99 System/Templates/Cards Anki.md`:
   ```markdown
   ---
   type: reference
   anki-deck: "Engenharia de Dados::Spark"
   tags: []
   created: AAAA-MM-DD
   updated: AAAA-MM-DD
   source: manual
   ---

   # Cards — Spark

   Cards do deck `Engenharia de Dados::Spark` no Anki. (…mesma frase do template…)

   ## Cards

   ### Spark: o que é uma transformação *wide*?
   Uma transformação que exige **shuffle**, porque cada partição de saída depende de várias de entrada (ex.: `groupBy`, `join`).

   ### Delta Lake: qual comando compacta arquivos pequenos de uma tabela?
   `OPTIMIZE`. Combine com `ZORDER BY` nas colunas mais filtradas.

   ## Relacionadas

   - [[Estudos com Anki]]
   - [[MOC - Spark & Databricks]]
   ```
   E acrescente a linha do deck na tabela `## Decks` do painel (`| Engenharia de Dados::Spark | [[Cards — Spark]] | [[MOC - Spark & Databricks]] |`).
4. **Nota de deck existente:** acrescente os cards **no fim de `## Cards`**, antes da próxima seção `##`. Não mexa nos cards nem nas linhas `%% anki: … %%` existentes. Antes de acrescentar, confira se já não há um card equivalente (`status`/leitura da nota).
5. Formato de cada card: `### <pergunta>?` numa linha e a resposta nas linhas seguintes (markdown simples: negrito, itálico, código, listas, links). A resposta nunca fica vazia. Sem `{{c1::…}}`: se aparecer, o `status` avisa, e o card deve ser reescrito como pergunta antes do envio.
6. Se a fonte é uma nota do vault, ponha a nota de deck em `## Relacionadas` da nota-fonte (regra 2 do vault permite) e a nota-fonte em `## Relacionadas` da nota de deck, respeitando o limite de 5.
7. Atualize `updated:` e **envie** (seção 3), a menos que ele peça só o rascunho. Mostre os cards criados em lista curta (frente → verso).

## 3. Enviar ao Anki (vault → Anki)

Quando ele pedir para enviar ou sincronizar, e ao fim da seção 2.

1. `anki_cards.py status` para o resumo e `anki_cards.py export` (padrão: `--only pendentes`, ou seja, novos + editados; aceita o nome da nota para limitar a um deck).
2. Para cada deck do export, garanta que ele existe (`list_decks`; se faltar, `create_deck`).
3. **Novos:** agrupe por deck e chame `add_notes` (até 100 por chamada), com:
   - o tipo básico detectado; `front` no primeiro campo (Frente), `back` no segundo (Verso);
   - nunca envie card com `kind: cloze` do export: reescreva-o como pergunta primeiro;
   - `tags` do export (`secondbrain` e `sb::<deck>`), que identificam no Anki os cards vindos do vault.
   Os campos já vêm em HTML; não reconverta.
4. Monte `[{"file", "key", "id"}]` só com os que deram certo (o `add_notes` tem sucesso parcial) e rode `anki_cards.py mark` passando esse JSON pela entrada padrão. Isso grava `%% anki: <id> <hash> %%` em cada card.
5. **Editados:** `update_notes` (ou `update_note_fields`) com o `anki_id` do export e os campos novos; depois `mark` com o mesmo `id`, para atualizar o hash. Edições no texto não mexem no agendamento.
6. Falhas: duplicata (o Anki recusa frente igual no mesmo tipo), campo inexistente, deck errado. Explique cada uma e não grave marcador para elas. Se a duplicata já existe no Anki com outro conteúdo, pergunte se ele quer ligar o card do vault ao ID existente.
7. Rode `status` de novo e confirme "novos 0, editados 0". Se ele usa AnkiWeb, ofereça (ou rode, se ele já autorizou no painel) a ferramenta `sync`, para os cards chegarem no celular.

## 4. Revisar

- **O normal é revisar no app do Anki** (desktop ou celular). Quando ele perguntar "o que tenho para revisar", leia `anki://stats/today` e responda com os devidos por deck e uma estimativa de tempo (~8 s por card).
- **Revisão pelo chat** (só se ele pedir, ex.: "me pergunta os cards de Spark"): use o prompt `review_session` do MCP ou `get_due_cards` → `present_card` (mostre só a frente) → espere a resposta dele → mostre o verso, comente o que faltou em uma linha → ele escolhe ou você sugere a nota (Errei / Difícil / Bom / Fácil) → `rate_card`. **Só avalie com a nota que ele confirmar.** Pare quando ele quiser ou ao zerar os devidos (padrão: 20 cards).
- Ao fim de uma revisão pelo chat, registre a sessão (seção 5).

## 5. Sessões e balanço

- **Registrar a sessão** (após revisar pelo chat, ou quando ele disser "revisei hoje"): leia `anki://stats/today` e acrescente uma linha no fim de `## Sessões` do painel (`Data | Deck | Revisados | Novos | Acerto | Observações`), com a data de hoje. Uma linha por dia; se já houver a de hoje, atualize-a. Não reescreva linhas antigas.
- **Balanço** ("como estão meus estudos?", na weekly ou a cada ~2 semanas): use `anki://stats/collection`, `anki://stats/forecast` e `cards_stats` dos decks (`tag:secondbrain` para os do vault). Traga em poucas linhas: dias estudados, acerto, carga dos próximos 7 dias, decks parados e **cards difíceis** (muitos lapsos ou facilidade baixa). Atualize `## Pontos fracos` do painel com os assuntos problemáticos (troque o marcador `_(nenhum ainda…)_` quando houver conteúdo).
- Para card difícil, proponha reescrever (dividir em cards menores, perguntar de forma mais específica, acrescentar exemplo). A reescrita é feita **na nota de deck** e enviada como "editado".
- Se a carga de revisão estiver alta, sugira diminuir cards novos por dia antes de criar mais cards.

## 6. Outras operações

- **Importar do Anki para o vault** (só se pedir): `find_notes` (ex.: `deck:"X" -tag:secondbrain`) + `notes_info`, escreva os cards na nota de deck (HTML convertido para markdown simples), rode `mark` com os IDs existentes e acrescente a tag `secondbrain`/`sb::<deck>` no Anki (`tag_add`).
- **Remover um card:** só com pedido explícito. Confirme, rode `delete_notes` no Anki e, no vault, mova o bloco do card para uma seção `## Removidos` no fim da nota de deck, com a linha `%% anki: … %%` trocada por `Removido em AAAA-MM-DD.` (o script só lê `## Cards`). Prefira **suspender** (`card_suspend`) quando ele só quiser parar de ver o card.
- **Ferramentas destrutivas** (`change_note_type`, `model_field_remove`) e mudanças de tipo de nota ou de template: só com pedido explícito e confirmação. Nunca rode `card_forget` ou `card_set_due_date` sem ele pedir.
- **Tarefas:** se ele quiser lembrete de revisão, crie pela skill `/tarefa` na área [[Pessoal]] (`#pessoal`, ex.: `🔁 every weekday`).

## 7. Finalizar

`git add` **só dos arquivos tocados** (notas de deck, painel, notas-fonte em que acrescentou links), commit com prefixo `estudos:` (ex.: `estudos: 8 cards de Spark enviados ao Anki`) e `git push`. Use a atribuição de commit que o ambiente indicar.

## 8. Trocar o tipo de um card já enviado

O tipo de nota (ex.: Cloze → Basic) não muda por edição: `change_note_type` é destrutivo e fica desligado. Para trocar, com o pedido dele:
1. Reescreva o card na nota de deck e **apague a linha `%% anki: … %%`** dele, para virar "novo".
2. Envie (seção 3) e grave o novo ID com `mark`.
3. Rode `delete_notes` no ID antigo, primeiro com `dry_run=true` e depois com `confirmDeletion=true`. O histórico de revisão do card antigo se perde: avise se ele já tinha revisões (`cards_stats`).
