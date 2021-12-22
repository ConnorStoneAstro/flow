# Flow

Flow is a Python library to abstract the development of large analysis tasks into a flowchart that processes some initial state.

## Installation

copy the 'flow' directory somewhere recognized by your python-path

## Usage

```python
import flow

# make a chart
mychart = flow.Chart('mychart')

# add nodes to the chart
mychart.add_process_node('myprocess1')
mychart.add_process_node('myprocess2')

# define the structure
mychart.link_nodes('start', 'myprocess1')
mychart.link_nodes('myprocess1', 'myprocess2')
mychart.link_nodes('myprocess2', 'end')

# draw a flowchart representation
mychart.draw('basic_chart.png')

# run the flowchart on an initial state
print(mychart({'initial': 'state'}))
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[GPL3](https://choosealicense.com/licenses/gpl-3.0/)