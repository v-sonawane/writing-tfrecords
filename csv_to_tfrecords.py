import apache_beam as beam
import tensorflow as tf

# Define the pipeline
with beam.Pipeline() as pipeline:
    # Read the CSV file and skip the header row
    data = (
        pipeline
        | 'Read CSV' >> beam.io.ReadFromText('dummy.csv', skip_header_lines=1)
        | 'Parse CSV' >> beam.Map(lambda line: line.split(','))
    )

    # Convert the data to TFRecords format
    def to_tfrecord(data):
        item_number = int(data[0])
        item_name = data[1]
        item_color = data[2]

        feature = {
            'item_number': tf.train.Feature(int64_list=tf.train.Int64List(value=[item_number])),
            'item_name': tf.train.Feature(bytes_list=tf.train.BytesList(value=[item_name.encode()])),
            'item_color': tf.train.Feature(bytes_list=tf.train.BytesList(value=[item_color.encode()]))
        }
        example = tf.train.Example(features=tf.train.Features(feature=feature))
        return example.SerializeToString()

    output_prefix = 'output'
    _ = (
        data
        | 'Convert to TFRecord' >> beam.Map(to_tfrecord)
        | 'Write to TFRecord' >> beam.io.WriteToTFRecord(
            output_prefix,
            file_name_suffix='.tfrecords'
        )
    )

