# Prefect 2 version of anaysis code template flow
import datetime
# import glob
import pprint
import sys
import time as ttime
import uuid

from bluesky_kafka import Publisher, RemoteDispatcher
import nslsii.kafka_utils

import numpy as np
from prefect import flow, get_run_logger, task
from tiled.client import from_profile

from ophyd.utils.epics_pvs import data_shape, data_type
from event_model import compose_run
from typing import Iterable, TypedDict


tiled_client = from_profile("nsls2", username=None)["cms"]
tiled_client_raw = tiled_client["raw"]
# tiled_client_processed = tiled_client["sandbox"]
cms_sandbox_tiled_client = tiled_client["bluesky_sandbox"]


kafka_config = nslsii.kafka_utils._read_bluesky_kafka_config_file(config_file_path="/etc/bluesky/kafka.yml")
reduced_producer = Publisher(
    key="",
    topic="cms.bluesky.reduced.documents",
    bootstrap_servers=",".join(kafka_config["bootstrap_servers"]),
    producer_config=kafka_config["runengine_producer_config"],
)


from SciAnalysis import tools
from SciAnalysis.XSAnalysis.Data import *
from SciAnalysis.XSAnalysis import Protocols
from SciAnalysis.Result import *

# TODO: where should this be?
# output_dir = "/nsls2/data/dssi/scratch/prefect-outputs/cms/"
# output_dir = "/nsls2/data/cms/legacy/xf11bm/data/2023_2/PTA/saxs/analysis/"


# Search for "TOCHANGE" below for where experiment-specific changes must be made

# Helpers
########################################
from collections.abc import MutableMapping

def flatten_dict(d: MutableMapping, parent_key: str = '', sep: str ='.') -> MutableMapping:
    # From:
    # https://www.freecodecamp.org/news/how-to-flatten-a-dictionary-in-python-in-4-different-ways/
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def reduce_run(bluesky_run, process, protocols, output_dir):
    """
    Reduce data from a single bluesky run.

    Parameters
    ----------
    bluesky_run : BlueskyRun
        The run to be reduced, assumed to be a v2 run object.

    Returns
    -------
    reduced : dict
        A single event

    metadata : dict
        Will be top-level in the reduced start document
    """
    print(f"give reduced on run {bluesky_run}")
    reduced = {}
    reduced["next big thing"] = "avocado toast"


    ########################################
    # SciAnalysis code goes here
    ########################################
    verbosity = 3
    if verbosity>=3:
        print("Starting SciAnalysis analysis...")

    # Determine filename
    dir = bluesky_run.metadata['start']['experiment_alias_directory']
    filename = bluesky_run.metadata['start']['filename']
    infile = '{}/maxs/raw/{}_maxs.tiff'.format(dir, filename) # TOCHANGE

    if verbosity>=3:
        print(f"Running SciAnalysis on: {infile}")


    # Access raw data
    #detector_image = bluesky_run["primary"]["data"]["pilatus2M_image"].read()
    #print(detector_image)
    if False:
        uid = bluesky_run.metadata['start']['uid']
        h = process.get_db(uid=uid)
        detector_image = h.table(fill=True)["pilatus2M_image"].to_numpy()


    # Run SciAnalysis
    process.run([infile], protocols, output_dir=output_dir, force=True)
    results_dict = ResultsDB(source_dir=output_dir).extract_single(infile, verbosity=verbosity)
    # value = results_dict['circular_average_q2I_fit']['fit_peaks_prefactor1']
    # #error = results_dict['circular_average_q2I_fit']['fit_peaks_prefactor1_error']
    # error = value*0.01
    # variance = np.square(error)

    if verbosity>=4:
        print("SciAnalysis generated results_dict:")
        #print(results_dict)
        pprint.pprint(results_dict)

    # Flatten results_dict to put it into reduced dict
    results_dict = flatten_dict(results_dict, sep="__")
    reduced.update(results_dict)

    # # for gpCAM
    # n = 1.0e0 # TOCHANGE Rescale values for gpCAM, so that they are roughly of order unity (this avoids machine precision problems)
    # reduced['value'] = value*n
    # reduced['variance'] = variance*n
    # reduced['analyzed'] = True

    ########################################
    # End of SciAnalysis specific code
    ########################################

    return reduced, {"raw_start": bluesky_run.metadata["start"]}


class DataKeys(TypedDict):
    dtype: str
    dtype_str: str
    dtype_descr: list
    shape: list


def infer_data_keys(doc: dict) -> DataKeys:
    data_keys = dict()
    _bad_iterables = (str, bytes, dict)
    _type_map = {
        "number": (float, np.floating, complex),
        "array": (np.ndarray, list, tuple),
        "string": (str,),
        "integer": (int, np.integer),
    }
    for key, val in doc.items():
        if val is None:
            _val = "None"
            dtype = "string"
        elif isinstance(val, Iterable) and not isinstance(val, _bad_iterables):
            _val = val
            dtype = "array"
        else:
            _val = val
            for json_type, py_types in _type_map.items():
                if isinstance(val, py_types):
                    dtype = json_type
                    break
            else:
                raise TypeError()
        arr_val = np.asanyarray(_val)
        arr_dtype = arr_val.dtype
        data_keys[key] = dict(
            dtype=dtype,
            dtype_str=arr_dtype.str,
            dtype_descr=arr_dtype.descr,
            shape=list(arr_val.shape),
            source="computed",
        )
    return data_keys


def publish_reduced_documents(reduced, metadata, reduced_publisher):
    logger = get_run_logger()
    cr = compose_run(metadata=metadata)
    reduced_publisher("start", cr.start_doc)
    logger.info(f"{reduced = }")

    desc_bundle = cr.compose_descriptor(
        name="primary",
        data_keys=infer_data_keys(reduced),
        # data_keys={
        #     k: {
        #         "dtype": data_type(v),
        #         "shape": data_shape(v),
        #         "source": "computed",
        #     }
        #     for k, v in reduced.items()
        # },
    )

    reduced_publisher("descriptor", desc_bundle.descriptor_doc)
    t = ttime.time()
    reduced_publisher(
        "event",
        desc_bundle.compose_event(
            data=reduced,
            timestamps={k: t for k in reduced},
        ),
    )

    reduced_publisher("stop", cr.compose_stop())

# def output_reduced_document(name, doc):
#     print(
#         f"{datetime.datetime.now().isoformat()} output document: {name}\n"
#         f"contents: {pprint.pformat(doc)}\n"
#     )


def output_reduced_document(name, doc):
    cms_sandbox_tiled_client.v1.insert(name, doc)
    reduced_producer(name, doc)


@task
def analysis(ref):
    logger = get_run_logger()
    run = tiled_client_raw[ref]
    full_uid = run.start["uid"]
    # logger.info(f"{full_uid = }")
    # FIXME: PTA could be in start doc or in md dict in start doc
    if not run.start.get("PTA") and not (run.start.get("md") and run.start.get("md").get("PTA")):
        logger.info(f"Not running analysis on {full_uid}")
        return
    else:
        logger.info(f"Running analysis on {full_uid}")



        ########################################
        # SciAnalsyis setup
        ########################################


        SciAnalysis_PATH='/nsls2/data/cms/legacy/xf11bm/software/SciAnalysis/'

        # TOCHANGE

        # # Experimental parameters
        # ########################################
        # TODO: can these be pulled from the bluesky start doc?
        calibration = Calibration(wavelength_A=0.9184)  # 13.5 keV; calibration wavelength_A
        calibration.set_image_size(981, height=1043) # Pilatus800k
        calibration.set_pixel_size(pixel_size_um=172.0)

        #calibration.set_beam_position(797, 594.5) # OpenWAXS ~240mm from sample in PTA setup # GIMP (796, 594)
        xshift = -20/0.172
        yshift = 40/0.172
        calibration.set_beam_position(797+xshift, 594.5+yshift) # OpenWAXS ~240mm from sample in PTA setup
        calibration.set_distance(0.255)

        # ************
        # TODO: what should all these paths be?
        #     - are they the same between experiments?
        #     - can I pull any of this from the bluesky start doc
        mask_dir = SciAnalysis_PATH + '/SciAnalysis/XSAnalysis/masks/'
        #mask = Mask(mask_dir+'Dectris/Pilatus2M_gaps-mask.png')
        mask = Mask(mask_dir+'Dectris/Pilatus800k2_gaps-mask.png')

        # /nsls2/data/cms/legacy/xf11bm/data/2023_1/KYager/code_test/saxs/analysis/mask.png
        # TODO: Try to pull this from bluesky start doc
        # experiment_alias_directory in start doc
        #analysis_dir = "/nsls2/data/cms/legacy/xf11bm/data/2023_2/PTA/saxs/analysis/"
        analysis_dir = "/nsls2/data/cms/legacy/xf11bm/data/2023_2/KChen-Wiegart2/maxs/analysisAE/" # TOCHANGE

        mask.load(analysis_dir + 'mask.png')
        #mask.load(analysis_dir + 'Pilatus2M_current-mask.png')

        # Analysis to perform
        ########################################
        # source_dir = f'{analysis_dir}/raw/'
        output_dir = analysis_dir
        # output_dir = "/nsls2/data/dssi/scratch/prefect-outputs/cms/"


        load_args = {'calibration': calibration,
                    'mask': mask,
                    # 'background': source_dir+'empty*saxs.tiff',
                    # 'transmission_int': f'{analysis_dir}/data/Transmission_output.csv', # Can also specify an float value.
                    }
        run_args = {'verbosity': 3,
                    # 'save_results': ['xml', 'plots', 'txt', 'hdf5'],
                    }

        process = Protocols.ProcessorXS(load_args=load_args, run_args=run_args)
        # TODO: below doesn't work and will not work. Prefect-worker1 can't
        #       access the cms databroker directly
        # process.connect_databroker('cms') # Access databroker metadata

        patterns = [
                    ['theta', '.+_th(\d+\.\d+)_.+'] ,
                    ['x_position', '.+_x(-?\d+\.\d+)_.+'] ,
                    ['y_position', '.+_yy(-?\d+\.\d+)_.+'] ,
                    #['anneal_time', '.+_anneal(\d+)_.+'] ,
                    #['cost', '.+_Cost(\d+\.\d+)_.+'] ,
                    ['annealing_temperature', '.+_T(\d+\.\d\d\d)C_.+'] ,
                    #['annealing_time', '.+_(\d+\.\d)s_T.+'] ,
                    #['annealing_temperature', '.+_localT(\d+\.\d)_.+'] ,
                    #['annealing_time', '.+_clock(\d+\.\d\d)_.+'] ,
                    #['o_position', '.+_opos(\d+\.\d+)_.+'] ,
                    #['l_position', '.+_lpos(\d+\.\d+)_.+'] ,
                    ['exposure_time', '.+_(\d+\.\d+)s_\d+_saxs.+'] ,
                    ['sequence_ID', '.+_(\d+).+'] ,
                    ]

        # TOCHANGE
        q_Al3Sc, dq_Al3Sc = 2.64, 0.05
        protocols = [
            #Protocols.HDF5(save_results=['hdf5'])
            #Protocols.calibration_check(show=False, AgBH=True, q0=0.010, num_rings=4, ztrim=[0.05, 0.05], ) ,
            #Protocols.circular_average(ylog=True, plot_range=[0, 0.12, None, None], label_filename=True) ,
            #Protocols.thumbnails(crop=None, resize=1.0, blur=None, cmap=cmap_vge, ztrim=[0.01, 0.001]) ,

            #Protocols.circular_average_q2I_fit(show=False, q0=0.0140, qn_power=2.5, sigma=0.0008, plot_range=[0, 0.06, 0, None], fit_range=[0.008, 0.022]) ,
            # Protocols.circular_average_q2I_fit(qn_power=3.5, trim_range=[0.005, 0.03], fit_range=[0.007, 0.019], q0=0.0120, sigma=0.0008) ,
            #Protocols.circular_average_q2I_fit(qn_power=3.0, trim_range=[0.005, 0.035], fit_range=[0.008, 0.03], q0=0.0180, sigma=0.001) ,

            # Protocols.linecut_qr_fit(show_region=False, show=False, qz=0.027, dq=0.008, fit_range=[0.008, 0.026], plot_range=[0, 0.05, 0, None]) ,
            # Protocols.linecut_qz_fit(qr=0.0185, dq=0.004, show_region=False, label_filename=True, trim_range=[0, 0.06], fit_range=[0.036, 0.055], plot_range=[0, 0.06, 0, None], q0=0.043, sigma=0.0022, critical_angle_substrate=0.132, critical_angle_film=0.094, ),

            #Protocols.linecut_qr_fit(show_region=False, show=False, qz=0.032, dq=0.008, fit_range=[0.008, 0.026], plot_range=[0, 0.05, 0, None]) ,
            # Protocols.linecut_qz_fit(name='linecut_qz_fit_p', qr=0.0195, dq=0.004, show_region=False, label_filename=True, trim_range=[0, 0.06], fit_range=[0.02, 0.055], plot_range=[0, 0.06, 0, None], q0=0.043, sigma=0.0022, critical_angle_substrate=0.132, critical_angle_film=0.094, ),
            # Protocols.linecut_qz_fit(name='linecut_qz_fit_bs', qr=0.0057, dq=0.0025, show_region=False, label_filename=True, trim_range=[0, 0.06], fit_range=[0.038, 0.058], plot_range=[0, 0.06, 0, None], q0=0.043, sigma=0.0022, critical_angle_substrate=0.132, critical_angle_film=0.094, ),

            #Protocols.databroker_extract(constraints={'measure_type':'measure'}, timestamp=True, sectino='start'),
            # Protocols.metadata_extract(patterns=patterns) ,

            Protocols.circular_average_q2I_fit(name='circular_average_q2I_fit_AE_Al3Sc', ylog=False, qn_power=1.0, plot_range=[1.0, 3.0, 0, None], fit_range=[q_Al3Sc-dq_Al3Sc, q_Al3Sc+dq_Al3Sc], dezing=True) ,

            ]


        ########################################
        # End SciAnalysis setup
        ########################################




        logger.info(f"reducing run {full_uid}")
        reduced, metadata = reduce_run(run, process, protocols, output_dir)
        logger.info(f"publishing run {full_uid}")
        publish_reduced_documents(reduced, metadata, output_reduced_document)
        logger.info("Done")


@flow
def analysis_flow(raw_ref):
    analysis(raw_ref)
