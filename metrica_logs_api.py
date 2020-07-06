from collections import namedtuple
import logs_api
import time
import clickhouse
import utils
import sys
import datetime
import logging

logger = logging.getLogger('logs_api')
config = utils.get_config()


def setup_logging():
    logging.basicConfig(stream=sys.stdout,
                        level=config['log_level'],
                        format='%(asctime)s %(processName)s %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S', )


def get_date_period(options):
    if options.mode is None:
        start_date_str = options.start_date
        end_date_str = options.end_date
    elif options.mode == 'regular':
        start_date_str = (datetime.datetime.today() - datetime.timedelta(2)) \
            .strftime(utils.DATE_FORMAT)
        end_date_str = (datetime.datetime.today() - datetime.timedelta(2)) \
            .strftime(utils.DATE_FORMAT)
    elif options.mode == 'regular_early':
        start_date_str = (datetime.datetime.today() - datetime.timedelta(1)) \
            .strftime(utils.DATE_FORMAT)
        end_date_str = (datetime.datetime.today() - datetime.timedelta(1)) \
            .strftime(utils.DATE_FORMAT)
    elif options.mode == 'history':
        start_date_str = utils.get_counter_creation_date(
            config['counter_id'],
            config['token']
        )
        end_date_str = (datetime.datetime.today() - datetime.timedelta(2)) \
            .strftime(utils.DATE_FORMAT)
    else:
        raise ValueError('Valid values for the @option.mode are: None, "regular", "regular_early", "history"')
    return start_date_str, end_date_str


def build_user_request():
    options = utils.get_cli_options()
    logger.info('CLI Options: ' + str(options))

    start_date_str, end_date_str = get_date_period(options)
    source = options.source

    # Validate that fields are present in config
    source_fields = f'{source}_fields'
    assert source_fields in config, 'Fields must be specified in config'
    fields = config[source_fields]

    # Creating data structure (immutable tuple) with initial user request
    UserRequest = namedtuple(
        "UserRequest",
        "token counter_id start_date_str end_date_str source fields"
    )

    user_request = UserRequest(
        token=config['token'],
        counter_id=config['counter_id'],
        start_date_str=start_date_str,
        end_date_str=end_date_str,
        source=source,
        fields=tuple(fields),
    )

    logger.info(user_request)
    utils.validate_user_request(user_request)
    return user_request


def integrate_with_logs_api(user_request):
    for i in range(config['retries']):
        time.sleep(i * config['retries_delay'])
        try:
            # Creating API requests
            api_requests = logs_api.get_api_requests(user_request)

            for api_request in api_requests:
                logger.info('### CREATING TASK')
                logs_api.create_task(api_request)
                # print(api_request)

                delay = 30
                while api_request.status != 'processed':
                    logger.info('### DELAY %d secs' % delay)
                    time.sleep(delay)
                    logger.info('### CHECKING STATUS')
                    api_request = logs_api.update_status(api_request)
                    logger.info('API Request status: ' + api_request.status)

                logger.info('### SAVING DATA')
                for part in range(api_request.size):
                    logger.info('Part #' + str(part))
                    logs_api.save_data(api_request, part)

                logger.info('### CLEANING DATA')
                logs_api.clean_data(api_request)
        except Exception as e:
            logger.critical(f'Iteration #{i + 1} failed')
            logger.critical(e)
            if i == config['retries'] - 1:
                raise e


def main():
    # print('##### python', utils.get_python_version())
    start_time = time.time()
    setup_logging()
    user_request = build_user_request()

    # If data for specified period is already in database, script is skipped
    if clickhouse.is_data_present(user_request.start_date_str,
                                  user_request.end_date_str,
                                  user_request.source):
        logging.critical('Data for selected dates is already in database')
        exit(0)

    integrate_with_logs_api(user_request)
    end_time = time.time()
    logger.info('### TOTAL TIME: %d minutes %d seconds' % (
        divmod(end_time - start_time, 60)
    ))


if __name__ == '__main__':
    main()
