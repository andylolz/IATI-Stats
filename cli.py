from datetime import datetime

import click
from rq import Queue

from worker import conn


@click.group()
def cli():
    pass


@cli.command()
def status():
    """Show how many tasks remain in each queue."""
    time_now = str(datetime.utcnow())
    click.echo('The time is: {}'.format(time_now))
    click.echo('')

    low_q = Queue('low', connection=conn)
    default_q = Queue('default', connection=conn)
    high_q = Queue('high', connection=conn)
    click.echo('Current queue status:')
    click.echo('      Low: {} tasks'.format(len(low_q)))
    click.echo('  Default: {} tasks'.format(len(default_q)))
    click.echo('     High: {} tasks'.format(len(high_q)))
    click.echo('')

    total_failed_low = len(low_q.failed_job_registry)
    total_failed_default = len(default_q.failed_job_registry)
    total_failed_high = len(high_q.failed_job_registry)
    click.echo('Failed queue status:')
    click.echo('      Low: {} tasks'.format(total_failed_low))
    click.echo('  Default: {} tasks'.format(total_failed_default))
    click.echo('     High: {} tasks'.format(total_failed_high))


@cli.command()
def clear():
    """Delete all tasks."""
    if not click.confirm('This will delete all tasks! Are you sure?'):
        click.echo('Quitting.')
        return
    q = Queue(connection=conn)
    q.delete(delete_jobs=True)
    click.echo('All tasks deleted.')


if __name__ == '__main__':
    cli()
