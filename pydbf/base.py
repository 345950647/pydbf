import datetime
import struct
import typing


def read_dbf_field_sizes(
        path: str,
        encoding: str = 'GB18030',
):
    with open(path, mode='rb') as fileobj:
        offset = struct.unpack('<8x1H22x', fileobj.read(32))[0]
        fields = struct.unpack(f'<{'11s5xB15x' * ((offset - 33) // 32)}', fileobj.read(offset - 33))

    field_sizes = {
        name.rstrip(b'\x00').decode(encoding): size
        for name, size in zip(fields[::2], fields[1::2])
    }

    return field_sizes


def write_dbf(
        path: str,
        field_sizes: dict[str, int],
        values: list[dict[str, str]],
        now=None,
        encoding: str = 'GB18030',
):
    if now is None:
        now = datetime.datetime.now()

    with open(
            path,
            mode='wb',
    ) as src:
        src.write(struct.pack(
            '<BBBBLHH20x',
            3,
            now.year - 1900,
            now.month,
            now.day,
            len(values),
            len(field_sizes) * 32 + 33,
            sum(field_sizes.values()) + 1,
        ))

        names_and_sizes = tuple(field_sizes.items())

        [src.write(struct.pack(
            '<11sc4xBB14x',
            name.encode(encoding).ljust(11, b'\x00'),
            b'C',
            size,
            0,
        )) for name, size in names_and_sizes]

        src.write(b'\r')

        [src.write(
            str(v[n])[:s].encode(encoding).ljust(s, b' ')
            if n in v
            else b' ' * s
        ) for v in values for n, s in (('', 1), *names_and_sizes)]

        src.write(b'\r\x1a')


def read_dbf(
        path: str,
        include: set = None,
        encoding: str = 'GB18030',
) -> typing.Tuple[typing.List[str], typing.List[typing.List]]:
    with open(path, mode='rb') as fileobj:
        num, offset, row_size = struct.unpack('<4x1L1H1H20x', fileobj.read(32))
        fields = struct.unpack(f'<{'11s5xB15x' * ((offset - 33) // 32)}', fileobj.read(offset - 33))

        if include is None:
            columns = [f.rstrip(b'\x00').decode(encoding) for f in fields[::2]]
            row_fmt = f'<1s{''.join([f'{s}s' for s in fields[1::2]])}'
        else:
            columns = []
            row_fmt = []

            for name, size in zip(fields[::2], fields[1::2]):
                name = name.rstrip(b'\x00').decode(encoding)
                if name in include:
                    columns.append(name)
                    row_fmt.append(f'{size}s')
                else:
                    row_fmt.append(f'{size}x')

            row_fmt = f'<1s{''.join(row_fmt)}'

        fileobj.seek(offset)

        data = [
            [x.rstrip(b' ').decode(encoding) for x in line[1:]]
            for line in
            (struct.unpack(row_fmt, fileobj.read(row_size)) for _ in range(num))
            if line[0] == b' '
        ]

    return columns, data
