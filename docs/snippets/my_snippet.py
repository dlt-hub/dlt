# import is outside of snipped but is needed for linting
import dlt 

# @@@SNIPSTART hellodlt
# some comment
p = dlt.Pipeline(pipeline_name='chess_pipeline')
p.sync_destination()
# @@@SNIPEND