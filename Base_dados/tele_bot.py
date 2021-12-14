#!/usr/bin/env python
# pylint: disable=C0116
# This program is dedicated to the public domain under the CC0 license.

"""
Simple Bot to reply to Telegram messages.
First, a few handler functions are defined. Then, those functions are passed to
the Dispatcher and registered at their respective places.
Then, the bot is started and runs until we press Ctrl-C on the command line.
Usage:
Basic Echobot example, repeats messages.
Press Ctrl-C on the command line or send a signal to the process to stop the
bot.
"""

import logging

from telegram import Update, ForceReply
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext
from basedados import*

# Enable logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)

logger = logging.getLogger(__name__)


# Define a few command handlers. These usually take the two arguments update and
# context.
def start(update: Update, _: CallbackContext) -> None:
    """Send a message when the command /start is issued."""
    user = update.effective_user
    update.message.reply_markdown_v2(
        fr'Hi {user.mention_markdown_v2()}\!',
        reply_markup=ForceReply(selective=True),
    )


def help_command(update: Update, _: CallbackContext) -> None:
    """Send a message when the command /help is issued."""
    update.message.reply_text("""

1. Lista de instruções do bot:

Logo após o comando, os textos utilizados são apenas exemplos a seguir e não exatamente o que você terá que escrever.
*OBS: os dados devolvidos pelo bot podem não ser os mais recentes, pois a atualização da base de dados não ocorre diariamente.
**OBS: as datas devem ser colocadas obedecendo o seguinte formato: mm/dd/aaaa


1.1. Comandos para ações:

1.1.1. /list_tickers
	*devolve arquivo xlsx com todos os tickers que estão disponíveis

1.1.2. /list_companies
	*devolve arquivo xlsx com dados das empresas listadas na B3 
	
1.1.3. /ticker_daily 'ITUB4' '01/01/2021' '07/07/2021' img
	*retorna uma imagem, csv ou xlsx com os dados diários do período especificado
	*o formato de imagem só está disponível para um período de no máximo 100 dias
	*se digitar apenas o nome do ticker, o output será uma imagem dos últimos dados díarios disponíveis
	
1.1.4. /ticker_graph_return 'ITUB4' '01/01/2021' '07/07/2021'
1.1.5. /ticker_graph_drawdown 'ITUB4' '01/01/2021' '07/07/2021'
1.1.6. /ticker_graph_volatility 'ITUB4' '01/01/2021' '07/07/2021'
1.1.6. /ticker_graph_return_application 'ITUB4' '01/01/2021' '07/07/2021'


1.2. Comando para índices

1.2.1. /list_index
    *devolve arquivo xlsx com todos os índices que estão disponíveis
    
1.2.2. /index_daily 'CDI''01/01/2021' '07/07/2021' img
    *retorna uma imagem, csv ou xlsx com os dados diários do período especificado
	*o formato de imagem só está disponível para um período de no máximo 100 dias
	*se digitar apenas o nome do índice, o output será uma imagem dos últimos dados díarios disponíveis
    


"""
)



def list_tickers_command(update: Update, context: CallbackContext) -> None:
    """Send a message when the command /list_tickers is issued."""

    list_tickers()

    path = '/home/ubuntu/arquivos/df.xlsx'
    update.message.reply_document(document=open(path, "rb"))


def list_index_command(update: Update, context: CallbackContext) -> None:
    """Send a message when the command /list_index is issued."""

    list_index()

    path = '/home/ubuntu/arquivos/df.xlsx'
    update.message.reply_document(document=open(path, "rb"))



def list_companies_command(update: Update, context: CallbackContext) -> None:
    """Send a message when the command /list_tickers is issued."""

    list_companies()

    path = '/home/ubuntu/arquivos/df.xlsx'
    update.message.reply_document(document=open(path, "rb"))



def ticker_daily_command(update: Update, context: CallbackContext) -> None:
    """Send a message when the command /list_tickers is issued."""
    try:
        ticker = context.args[0]
        datainicial = context.args[1]
        datafinal = context.args[2]
        formato = context.args[3]


        objt = ticker_daily(ticker, datainicial, datafinal, formato)


        if formato == 'xlsx':
            path = '/home/ubuntu/arquivos/df_daily.xlsx'
            update.message.reply_document(document=open(path, "rb"))

        elif formato == 'csv':
            path = '/home/ubuntu/arquivos/df_daily.csv'
            update.message.reply_document(document=open(path, "rb"))

        elif formato == 'img':
            if len(objt)<=100:
                path = '/home/ubuntu/arquivos/df_daily.png'
                update.message.reply_photo(photo=open(path, "rb"))
            else:
                update.message.reply_text("O período requisitado tem mais de 100 dias")
    except IndexError:
        ticker = context.args[0]
        get_ticker(ticker)

        path = '/home/ubuntu/arquivos/df.png'
        update.message.reply_photo(photo=open(path, "rb"))




def index_daily_command(update: Update, context: CallbackContext) -> None:
    """Send a message when the command /list_tickers is issued."""
    try:
        ticker = context.args[0]
        datainicial = context.args[1]
        datafinal = context.args[2]
        formato = context.args[3]


        objt = index_daily(ticker, datainicial, datafinal, formato)


        if formato == 'xlsx':
            path = '/home/ubuntu/arquivos/df_daily.xlsx'
            update.message.reply_document(document=open(path, "rb"))

        elif formato == 'csv':
            path = '/home/ubuntu/arquivos/df_daily.csv'
            update.message.reply_document(document=open(path, "rb"))

        elif formato == 'img':
            if len(objt)<=100:
                path = '/home/ubuntu/arquivos/df_daily.png'
                update.message.reply_photo(photo=open(path, "rb"))
            else:
                update.message.reply_text("O período requisitado tem mais de 100 dias")

    except IndexError:
        ticker = context.args[0]
        get_index(ticker)

        path = '/home/ubuntu/arquivos/df.png'
        update.message.reply_photo(photo=open(path, "rb"))


def drawdown_command(update: Update, context: CallbackContext) -> None:

        ticker = context.args[0]
        datainicial = context.args[1]
        datafinal = context.args[2]



        df = ticker_daily(ticker, datainicial, datafinal, formato="csv")
        df1 = index_daily("'CDI'", datainicial, datafinal, formato = "csv")
        print(drawdown(df, df1))

        path = '/home/ubuntu/arquivos/Drawdown.png'
        update.message.reply_photo(photo=open(path, "rb"))




def volatility_command(update: Update, context: CallbackContext) -> None:
    ticker = context.args[0]
    datainicial = context.args[1]
    datafinal = context.args[2]

    df = ticker_daily(ticker, datainicial, datafinal, formato="csv")
    print(volatility(df))

    path = '/home/ubuntu/arquivos/Volatility.png'
    update.message.reply_photo(photo=open(path, "rb"))



def return_command(update: Update, context: CallbackContext) -> None:
    ticker = context.args[0]
    datainicial = context.args[1]
    datafinal = context.args[2]

    df = ticker_daily(ticker, datainicial, datafinal, formato="csv")
    df1 = index_daily("'CDI'", datainicial, datafinal, formato="csv")
    print(returns(df, df1))

    path = '/home/ubuntu/arquivos/Return.png'
    update.message.reply_photo(photo=open(path, "rb"))



def return_application_command(update: Update, context: CallbackContext) -> None:
    ticker = context.args[0]
    datainicial = context.args[1]
    datafinal = context.args[2]

    df = ticker_daily(ticker, datainicial, datafinal, formato="csv")
    print(return_application(df))

    path = '/home/ubuntu/arquivos/Return_application.png'
    update.message.reply_photo(photo=open(path, "rb"))




def echo(update: Update, _: CallbackContext) -> None:
    """Echo the user message."""
    update.message.reply_text(update.message.text)


def main() -> None:
    """Start the bot."""
    # Create the Updater and pass it your bot's token.
    updater = Updater(token)

    # Get the dispatcher to register handlers
    dispatcher = updater.dispatcher

    # on different commands - answer in Telegram
    dispatcher.add_handler(CommandHandler("start", start))
    dispatcher.add_handler(CommandHandler("help", help_command))
    dispatcher.add_handler(CommandHandler("list_tickers", list_tickers_command))
    dispatcher.add_handler(CommandHandler("list_index", list_index_command))
    dispatcher.add_handler(CommandHandler("list_companies", list_companies_command))
    dispatcher.add_handler(CommandHandler("ticker_daily", ticker_daily_command))
    dispatcher.add_handler(CommandHandler("index_daily", index_daily_command))
    dispatcher.add_handler(CommandHandler("ticker_graph_drawdown", drawdown_command))
    dispatcher.add_handler(CommandHandler("ticker_graph_volatility", volatility_command))
    dispatcher.add_handler(CommandHandler("ticker_graph_return", return_command))
    dispatcher.add_handler(CommandHandler("ticker_graph_return_application", return_application_command))

    # on non command i.e message - echo the message on Telegram
    dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, echo))

    # Start the Bot
    updater.start_polling()

    # Run the bot until you press Ctrl-C or the process receives SIGINT,
    # SIGTERM or SIGABRT. This should be used most of the time, since
    # start_polling() is non-blocking and will stop the bot gracefully.
    updater.idle()






if __name__ == '__main__':
    main()
    print("Inciado com sucesso!")
