import logging
import azure.functions as func
import LogAutomation
import queries_list
app = func.FunctionApp()

@app.schedule(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def timer_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')
    logging.info('Python timer trigger function executed.')

@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def dailylogcountfunction(myTimer: func.TimerRequest) -> None:
    
    if myTimer.past_due:
        logging.info('The timer is past due!')

    LogAutomation.execute_queries(LogAutomation.log_count_queries,LogAutomation.error_count_queries,"lock-error-count.xlsx")
    logging.info('Python timer trigger function executed.')