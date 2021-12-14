# My_projects

![Banner](https://wallpaperaccess.com/full/2703803.jpg)

Just a taste of some of my own code projects in python.

**1) Análise_Beta**: Script criado para testar uma estratégia que se baseia no cálculo do valor esperado de um ativo pela clássica fórmula do CAPM.
Após chegar no valor esperado do ativo em questão, ele é comparado ao valor real que ele atingiu. Caso o valor real seja maior do que se era esperado, significa que naquele momento ele está caro, portanto, recomenda-se não comprá-lo. Em compensação, caso o valor real seja menor do que se era esperado, significa que ele está barato e, consequentemente, possui sinal verde para a compra do mesmo.
Muito importante enfatizar que essa estratégia visa principalmente a diminuição de perdas e não a maximização dos retornos, ou seja, se uma certa carteira tiver performance negativa, ao aplicar essa estratégia, e selecionar apenas algumas ações desta carteira, essas ações selecionadas darão perdas menores do que se considerasse a carteira toda.


**2) Base_dados**: Dentro dessa pasta, há um scirpt chamado basedados.py e outro chamado tele_bot.py. A ideia era que a partir desses dois scripts fosse possível criar uma base de dados postgresql (hospedado na cloud), contendo dados do mercado financeiro extraídos da plataforma da Comdinheiro; e criar um bot do telegram que daria um output de gráficos informativos construídos a partir dos dados dentro dessa base, ou simplesmente só retornasse os dados de forma pura.
O basedados.py contém as funções necessárias para extrair dados do comdinheiro, dar input dos dados no postgresql, criar os gráficos financeiros.
O tele_bot.py contém os comandos de função do bot do telegram.
Além disso, há outros arquivos secundários na pasta, como triggers do postgresql para realizar boas práticas na entrada dos dados e um esquema de como foi montada a base.

*Link de tutorial utilizado para criar a base na aws:* https://www.youtube.com/watch?v=VLpPLaGVJhI&list=LLL-iV73KQiBOciWHOBy8x-w&index=9

*Link de como rodar o código no vm:* https://www.youtube.com/watch?v=BYvKv3kM9pk&t=756s

*Link de como usar bot do telegram com python:* https://www.youtube.com/watch?v=hOow3Omxm2U&t=937s
