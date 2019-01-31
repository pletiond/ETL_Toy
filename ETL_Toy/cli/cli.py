import click

from ETL_Toy.jobs.transformation import Transformation


@click.command()
@click.option('--data_targets', '-d', default=None, show_default=True, help='File with data targets')
@click.option('--log', '-l', default='', help='Target file for logging')
@click.argument('TARGET', nargs=-1)
def main(data_targets, log, target):
    """
    CLI function. Run whole program

    @:param data_targets: File with data targets
    @param log: Relative path to log file
    @param target: Target file including transformation
    """

    if not len(target) == 1:
        mess = click.style('Select 1 target!', fg='red')
        click.echo(mess)
        exit(1)
    try:
        target_object = Transformation.load(target[0])
    except FileNotFoundError as e:
        mess = click.style('Target file doesnt exist!', fg='red')
        click.echo(mess)
        exit(1)

    target_object.run(log, data_targets)


if __name__ == '__main__':
    main()
