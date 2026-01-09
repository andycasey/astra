import scipy.optimize as op
from scipy import linalg, interpolate
import os
import pickle
import sys
import numpy as np
import matplotlib.pyplot as plt
from astropy.io import fits
import lmfit
from typing import Iterable, Optional, Union
import pandas as pd

from astra import __version__, task
from astra.utils import log, expand_path
from peewee import JOIN
from astra.models.mwm import BossCombinedSpectrum
from astra.models.boss import BossVisitSpectrum
from astra.models.source import Source
from astra.models.spectrum import SpectrumMixin
from astra.models.snow_white import SnowWhite

from joblib import parallel_config


PIPELINE_DATA_DIR = expand_path(f"$MWM_ASTRA/pipelines/snow_white")
LARGE = 1e3

from tqdm import tqdm

@task
def snow_white_filter(spectra: Iterable[BossCombinedSpectrum], **kwargs) -> Iterable[SnowWhite]:
    for spectrum in spectra:
        if "mwm_wd" not in spectrum.source.sdss5_cartons["program"]:
            yield SnowWhite.from_spectrum(spectrum, flag_not_mwm_wd=True)

@task
def snow_white(
    spectra: Iterable[BossCombinedSpectrum],
    debug=False,
    plot=True,
    **kwargs
) -> Iterable[SnowWhite]:
    """
    Classify white dwarf types based on their spectra, and fit stellar parameters to DA-type white dwarfs.

    :param spectra:
        Input spectra.
    """

    from astra.pipelines.snow_white import get_line_info_v3, fitting_scripts
    def refit(fit_params,spec_nl,spec_w,emu,wref):
        first_T=fit_params['teff'].value
        if first_T>=16000 and first_T<=40000:
            line_crop = np.loadtxt(os.path.join(PIPELINE_DATA_DIR, 'line_crop.dat'),skiprows=1,max_rows=4) #exclude Halpha. It is needed in exception
        elif first_T>=8000 and first_T<16000:
            line_crop = np.loadtxt(os.path.join(PIPELINE_DATA_DIR, 'line_crop_cool.dat'),skiprows=1,max_rows=4)
        elif first_T<8000:
            line_crop = np.loadtxt(os.path.join(PIPELINE_DATA_DIR, 'line_crop_vcool.dat'),max_rows=5)
        elif first_T>40000:
            line_crop = np.loadtxt(os.path.join(PIPELINE_DATA_DIR, 'line_crop_hot.dat'),skiprows=1,max_rows=4)
        l_crop = line_crop[(line_crop[:,0]>spec_w.min()) & (line_crop[:,1]<spec_w.max())]
        new_best= lmfit.minimize(fitting_scripts.line_func_rv,fit_params,args=(spec_nl,l_crop,emu,wref),method="least_squares",loss='soft_l1')
        return(new_best)
    #with open(os.path.join(PIPELINE_DATA_DIR, 'training_file_v3'), 'rb') as f:
    with open(os.path.join(PIPELINE_DATA_DIR, '20240801_training_file'), 'rb') as f:
        kf = pickle.load(f, fix_imports=True)

    kf.verbose = 0

    wref = np.load(os.path.join(PIPELINE_DATA_DIR, "wref_sdss.npy"))

    # Once again, we hhave to put this stupid hack in
    sys.path.insert(0, os.path.dirname(__file__))
    with open(os.path.join(PIPELINE_DATA_DIR, "emu_file_sdss"), 'rb') as pickle_file:
        emu = pickle.load(pickle_file)

    for spectrum in spectra:

        if not spectrum.source.assigned_to_program("mwm_wd"):
            yield SnowWhite.from_spectrum(spectrum, flag_not_mwm_wd=True)
            continue

        try:
            if np.sum(spectrum.flux) == 0:
                raise ValueError("bad")
        except:
            yield SnowWhite.from_spectrum(spectrum, flag_no_flux=True)
            continue


        try:
            bad_pixel = (
                (spectrum.flux == 0)
            |   (spectrum.ivar == 0)
            |   (~np.isfinite(spectrum.flux))
            )
            flux = np.copy(spectrum.flux)
            flux[bad_pixel] = 0.01
            e_flux = np.copy(spectrum.e_flux)
            e_flux[bad_pixel] = LARGE

            data_args = (spectrum.wavelength, flux, e_flux)

            labels = get_line_info_v3.line_info(*data_args)
            predictions = kf.predict(labels.reshape(1, -1))
            probs = kf.predict_proba(labels.reshape(1, -1))
            prob_arr = probs[0]
            idx_pred = np.where(kf.classes_ == predictions[0])[0][0]
            first = prob_arr[idx_pred]
            #first = probs[0][kf.classes_==predictions[0]]
            if first >= 0.5:
                classification = predictions[0]
            else:
                sorted_idx = np.argsort(prob_arr)
                second_idx = sorted_idx[-2]
                second = prob_arr[second_idx]
                if second/first > 0.6:
                    classification = f"{predictions[0]}/{kf.classes_[second_idx]}"
                else:
                    classification = predictions[0] + ":"
                #second = sorted(probs[0])[-2]
                #if second/first>0.6:
                #    classification = predictions[0]+"/"+kf.classes_[probs[0]==second]
                #else:
                #    classification = predictions[0]+":"

            result_kwds = dict(
                source_pk=spectrum.source_pk,
                spectrum_pk=spectrum.spectrum_pk,
                classification=classification,
            )
            result_kwds.update(
                dict(zip([f"p_{class_name.lower()}" for class_name in kf.classes_], probs[0]))
            )

            if classification not in ("DA", "DA:"):
                result = SnowWhite(**result_kwds)

            else:
                # Fit DA-type
                spec_stack=np.stack(data_args,axis=-1)
                spec_stack = spec_stack[(~np.isnan(spec_stack[:,1])) & (spec_stack[:,0] > 3600) & (spec_stack[:,0] < 9800)]
                spec_w = spec_stack[:, 0].copy()


                #normilize spectrum
                spec_n, cont_flux = fitting_scripts.norm_spectra(spec_stack,mod=False)
                #load lines to fit and crops them
                line_crop = np.loadtxt(os.path.join(PIPELINE_DATA_DIR, 'line_crop.dat'))
                l_crop = line_crop[(line_crop[:,0]>spec_w.min()) & (line_crop[:,1]<spec_w.max())]

                #fit entire grid to find good starting point
                lines_sp,lines_mod,best_grid,grid_param,grid_chi=fitting_scripts.fit_grid(spec_n,line_crop)

                #first_T=grid_param[grid_chi==np.min(grid_chi)][0][0]
                idx_best = np.argmin(grid_chi)
                first_T = grid_param[idx_best][0]
                first_g=800
                initial=0
                tl= pd.read_csv(os.path.join(PIPELINE_DATA_DIR, 'reference_phot_tlogg.csv'))
                sourceID=np.array(tl['source_id']).astype(str)
                T_H=np.array(tl['teff_H']).astype(float)
                log_H=np.array(tl['logg_H']).astype(float)
                eT_H=np.array(tl['eteff_H']).astype(float)
                elog_H=np.array(tl['elogg_H']).astype(float)
                GaiaID=str(spectrum.source.gaia_dr3_source_id)
                if GaiaID in sourceID: #if there is a photometric solution use that as starting point
                    first_T=T_H[sourceID==GaiaID][0]
                    first_g=log_H[sourceID==GaiaID][0]*100
                    initial=1


                if first_T > 120000:
                    first_T=120000
                if first_g < 601:
                    first_g=601
                if first_T>=16000 and first_T<=40000:
                    line_crop = np.loadtxt(os.path.join(PIPELINE_DATA_DIR, 'line_crop.dat'),skiprows=0,max_rows=5) #exclude Halpha. It is needed in exception
                elif first_T>=8000 and first_T<16000:
                    line_crop = np.loadtxt(os.path.join(PIPELINE_DATA_DIR, 'line_crop_cool.dat'),skiprows=0,max_rows=6)
                elif first_T<8000:
                    line_crop = np.loadtxt(os.path.join(PIPELINE_DATA_DIR, 'line_crop_vcool.dat'),skiprows=0,max_rows=5)
                elif first_T>40000:
                    line_crop = np.loadtxt(os.path.join(PIPELINE_DATA_DIR, 'line_crop_hot.dat'),skiprows=0,max_rows=5)
                l_crop = line_crop[(line_crop[:,0]>spec_w.min()) & (line_crop[:,1]<spec_w.max())]


                # initiate parameters for the fit
                fit_params = lmfit.Parameters()
                fit_params['teff'] = lmfit.Parameter(name="teff",value=first_T,min=4000,max=120000)
                fit_params['logg'] = lmfit.Parameter(name="logg",value=first_g,min=601,max=949)
                fit_params['rv'] = lmfit.Parameter(name="rv",value=0.2, min=-80, max=80) #this is a wavelenght shift not a radial velocity. since a eparate module finds rv

                #new normalization rotine working just on the balmer lines
                spec_nl=fitting_scripts.da_line_normalize(spec_stack,l_crop,mod=False)

                #this calls the scripts in fitting_scipts and does the actual fitting
                new_best= lmfit.minimize(fitting_scripts.line_func_rv,fit_params,args=(spec_nl,l_crop,emu,wref),method="least_squares",loss='soft_l1')
                #problematic nodes results can sometimes be fixed by excluding certain lines
                prob_list=[7.6,7.9,8.0,8.1,8.2,8.3,8.4,8.5,8.7,8.9]
                logg_val = new_best.params['logg'].value / 100.0
                if any(np.isclose(logg_val, p, atol=1e-6) for p in prob_list):
                    fit_params['logg'] = lmfit.Parameter(name="logg", value=800, min=601, max=949)
                    new_best = refit(fit_params, spec_nl, spec_w, emu, wref)
                best_T=new_best.params['teff'].value
                #best_Te=new_best.params['teff'].stderr
                best_g=new_best.params['logg'].value
                #best_ge=new_best.params['logg'].stderr
                shift=new_best.params['rv'].value
                chi2=new_best.redchi #can easily get a chi2
                #refit using solution and leastsq to get uncertainties
                err_params = lmfit.Parameters()
                err_params['teff'] = lmfit.Parameter(name="teff",value=best_T,min=4000,max=120000)
                err_params['logg'] = lmfit.Parameter(name="logg",value=best_g,min=601,max=949)
                err_params['rv']= lmfit.Parameter(name="rv",value=shift, min=-80, max=80)
                err_best=lmfit.minimize(fitting_scripts.line_func_rv,err_params,args=(spec_nl,l_crop,emu,wref),method="leastsq")
                best_Te=err_best.params['teff'].stderr
                best_ge=err_best.params['logg'].stderr

                if best_Te is None:
                    best_Te=0.0
                if best_ge is None:
                    best_ge=0.0
                if initial ==1:
                    result_kwds.update(teff=best_T,e_teff=best_Te,logg=best_g/100,e_logg=best_ge/100)

                elif initial == 0: #if initial guess not from photometric result need to repeat for hot/cold solution
                    if (spectrum.source.bp_mag is None or spectrum.source.rp_mag is None):
                        yield SnowWhite.from_spectrum(spectrum, flag_missing_bp_rp_mag=True)
                        continue

                    fit_params = lmfit.Parameters()
                    fit_params['logg'] = lmfit.Parameter(name="logg",value=800,min=601,max=949) #stick with logg 800
                    fit_params['rv'] = lmfit.Parameter(name="rv",value=0.2, min=-80, max=80)

                    if first_T <=13000.:
                        tmp_Tg,tmp_chi= grid_param[grid_param[:,0]>13000.], grid_chi[grid_param[:,0]>13000.]
                        idx_best2 = np.argmin(tmp_chi)
                        second_T = tmp_Tg[idx_best2][0]
                        #second_T= tmp_Tg[tmp_chi==np.min(tmp_chi)][0][0]
                        fit_params['teff'] = lmfit.Parameter(name="teff",value=second_T,min=12000,max=120000)

                    elif first_T >13000.:
                        tmp_Tg,tmp_chi= grid_param[grid_param[:,0]<13000.], grid_chi[grid_param[:,0]<13000.]
                        second_T= tmp_Tg[tmp_chi==np.min(tmp_chi)][0][0]
                        fit_params['teff'] = lmfit.Parameter(name="teff",value=second_T,min=4000,max=14000)

                    if second_T>=16000 and second_T<=40000:
                        line_crop = np.loadtxt(os.path.join(PIPELINE_DATA_DIR, 'line_crop.dat'),skiprows=0,max_rows=5)
                    elif second_T>=8000 and second_T<16000:
                        line_crop = np.loadtxt(os.path.join(PIPELINE_DATA_DIR, 'line_crop_cool.dat'),skiprows=0,max_rows=5)
                    elif second_T<8000:
                        line_crop = np.loadtxt(os.path.join(PIPELINE_DATA_DIR, 'line_crop_vcool.dat'),skiprows=0,max_rows=6)
                    elif second_T>40000:
                        line_crop = np.loadtxt(os.path.join(PIPELINE_DATA_DIR, 'line_crop_hot.dat'),skiprows=0,max_rows=5)
                    l_crop = line_crop[(line_crop[:,0]>spec_w.min()) & (line_crop[:,1]<spec_w.max())]

                #====================find second solution ==============================================
                    second_best= lmfit.minimize(fitting_scripts.line_func_rv,fit_params,args=(spec_nl,l_crop,emu,wref),method="least_squares",loss='soft_l1')
                    logg_val2 = second_best.params['logg'].value / 100.0
                    if any(np.isclose(logg_val2, p, atol=1e-6) for p in prob_list):
                        second_best = refit(fit_params, spec_nl, spec_w, emu, wref)
                    best_T2=second_best.params['teff'].value
                    #best_Te2=second_best.params['teff'].stderr
                    best_g2=second_best.params['logg'].value
                    #best_ge2=second_best.params['logg'].stderr
                    shift2=second_best.params['rv'].value
                    chi2_2=second_best.redchi #can easily get a chi2
                    #refit using solution and leastsq to get uncertainties
                    err_params2 = lmfit.Parameters()
                    err_params2['teff'] = lmfit.Parameter(name="teff",value=best_T2,min=4000,max=120000)
                    err_params2['logg'] = lmfit.Parameter(name="logg",value=best_g2,min=601,max=949)
                    err_params2['rv']= lmfit.Parameter(name="rv",value=shift2, min=-80, max=80)
                    err_best2=lmfit.minimize(fitting_scripts.line_func_rv,err_params2,args=(spec_nl,l_crop,emu,wref),method="leastsq")
                    best_Te2=err_best2.params['teff'].stderr
                    best_ge2=err_best2.params['logg'].stderr
                    if best_Te2 is None:
                        best_Te2=0.0
                    if best_ge2 is None:
                        best_ge2=0.0
                #========================use gaia G mag and parallax to solve for hot vs cold solution

                    #T_true=fitting_scripts.hot_vs_cold(best_T,best_g/100,best_T2,best_g2/100,spectrum.source.plx or np.nan,spectrum.source.g_mag or np.nan,emu,wref)
                    #new function uses bp-rp colour instead of parallax and G magnitude.
                    bp_rp=spectrum.source.bp_mag-spectrum.source.rp_mag
                    T_true=fitting_scripts.hot_vs_cold_col(best_T,best_g/100,best_T2,best_g2/100,bp_rp,emu,wref)

                    if T_true==best_T:
                        result_kwds.update(
                            teff=best_T,
                            e_teff=best_Te,
                            logg=best_g/100,
                            e_logg=best_ge/100,
                            #v_rel=best_rv  # rv should not be an output of snow_white now
                        )
                    elif T_true==best_T2:
                        result_kwds.update(
                            teff=best_T2,
                            e_teff=best_Te2,
                            logg=best_g2/100,
                            e_logg=best_ge2/100,
                            #v_rel=best_rv2
                        )
                if spectrum.snr <= 8:
                    result_kwds["flag_low_snr"] = True

                result = SnowWhite(**result_kwds)

#=========================================================still use old fit_func to generateretrieve model for plot==================================================

                # Get and save the 2 best lines from the spec and model, and the full models
                lines_s,lines_m,mod_n=fitting_scripts.fit_func(
                    (best_T,best_g,shift),
                    spec_n,l_crop,emu,wref,mode=1
                )

                #full_spec=np.stack(data_args,axis=-1)
                full_spec = spec_stack[(~np.isnan(spec_stack[:,1])) & (spec_stack[:,0] > 3500) & (spec_stack[:,0] < 7900)]


                # Adjust the flux of models to match the spectrum
                check_f_spec=full_spec[:,1][(full_spec[:,0]>4500.) & (full_spec[:,0]<4550.)]
                check_f_model=mod_n[:,1][(mod_n[:,0]>4500.) & (mod_n[:,0]<4550.)]
                adjust=np.average(check_f_model)/np.average(check_f_spec)

                model_wavelength, model_flux = (mod_n[:,0]+shift, (mod_n[:,1]/adjust))
                # resample
                resampled_model_flux = interpolate.interp1d(model_wavelength, model_flux, kind='linear', bounds_error=False)(spectrum.wavelength)

                output_path = expand_path(result.intermediate_output_path)
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                fits.HDUList([
                    fits.PrimaryHDU(),
                    fits.ImageHDU(resampled_model_flux)
                    ]
                ).writeto(output_path, overwrite=True)

                if plot:
                    if initial==0:
                        lines_s_o,lines_m_o,mod_n_o=fitting_scripts.fit_func((best_T2,best_g2,shift2),
                                                                    spec_n,l_crop,emu,wref,mode=1)
                    fig=plt.figure(figsize=(8,5))
                    ax1 = plt.subplot2grid((3,4), (0, 3),rowspan=3)
                    step = 0
                    for i in range(0,len(lines_s)): # plots Halpha (i=0) to H6 (i=5)
                        min_p   = lines_s[i][:,0][lines_s[i][:,1]==np.min(lines_s[i][:,1])][0]
                        ax1.plot(lines_s[i][:,0]-min_p,lines_s[i][:,1]+step,color='k')
                        ax1.plot(lines_s[i][:,0]-min_p,lines_m[i]+step,color='r')
                        if initial ==0:
                            min_p_o = lines_s_o[i][:,0][lines_s_o[i][:,1]==np.min(lines_s_o[i][:,1])][0]
                            ax1.plot(lines_s_o[i][:,0]-min_p_o,lines_m_o[i]+step,color='g')
                        step+=0.5
                    xticks = ax1.xaxis.get_major_ticks()
                    ax1.set_xticklabels([])
                    ax1.set_yticklabels([])

                    ax2 = plt.subplot2grid((3,4), (0, 0),colspan=3,rowspan=2)
                    ax2.plot(full_spec[:,0],full_spec[:,1],color='k')
                    ax2.plot(mod_n[:,0]+shift,(mod_n[:,1]/adjust),color='r')
                    if initial ==0:
                        check_f_model_o=mod_n_o[:,1][(mod_n_o[:,0]>4500.) & (mod_n_o[:,0]<4550.)]
                        adjust_o=np.average(check_f_model_o)/np.average(check_f_spec)
                        ax2.plot(mod_n_o[:,0]+shift2,mod_n_o[:,1]/adjust_o,color='g')

                    ax2.set_ylabel(r'F$_{\lambda}$ [erg cm$^{-2}$ s$^{-1} \AA^{-1}$]',fontsize=12)
                    ax2.set_xlabel(r'Wavelength $(\AA)$',fontsize=12)
                    ax2.set_xlim([3400,5600])
                    ax2.set_ylim(0, 2 * np.nanmax(mod_n[:,1]/adjust))
                    ax3 = plt.subplot2grid((3,4), (2, 0),colspan=3,rowspan=1,sharex=ax2)

                    flux_i = interpolate.interp1d(mod_n[:,0]+shift,mod_n[:,1]/adjust,kind='linear', bounds_error=False)(full_spec[:,0])
                    wave3=full_spec[:,0]
                    flux3=full_spec[:,1]/flux_i
                    binsize=1
                    xdata3=[]
                    ydata3=[]
                    for i in range(0,(np.size(wave3)-binsize),binsize):
                        xdata3.append(np.average(wave3[i:i+binsize]))
                        ydata3.append(np.average(flux3[i:i+binsize]))
                    plt.plot(xdata3,ydata3)

                    plt.hlines(1.02, 3400,5600,colors="r")
                    plt.hlines(1.01, 3400,5600,colors="0.5",ls="--")
                    plt.hlines(0.98, 3400,5600,colors="r")
                    plt.hlines(0.99, 3400,5600,colors="0.5",ls="--")
                    ax3.set_xlim([3400,5600])
                    ax3.set_ylim([0.95,1.04])

                    figure_path = output_path[:-4] + ".png"
                    fig.savefig(figure_path)
                    plt.close("all")

            # No chi2, statistics, or flagging information..
            yield result

        except:
            log.exception(f"Exception on spectrum={spectrum}")
            if debug:
                raise
