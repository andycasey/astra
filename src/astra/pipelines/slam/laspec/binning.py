import numpy as np

from .wavelength import wave_log10


def center2edge(x):
    x = np.asarray(x)
    dx = np.diff(x)
    return np.hstack((x[0] - .5 * dx[0], x[:-1] + .5 * dx, x[-1] + .5 * dx[-1]))


def rebin(wave, flux=None, flux_err=None, mask=None, wave_new=None):
    """ Rebin spectrum to a new wavelength grid

    Parameters
    ----------
    wave: array
        old wavelength
    flux: array
        old flux
    flux_err: array (optional)
        old flux error
    mask: array (optional)
        old mask, True for bad.
    wave_new:
        new wavelength. if None, use log10 wavelength.

    Return
    ------
    re-binned (flux, [flux_err], [mask])
    """
    wave = np.asarray(wave)
    if wave_new is None:
        wave_new = wave_log10(wave)
    else:
        wave_new = np.asarray(wave_new)

    wave_edge = center2edge(wave)
    wave_new_edge = center2edge(wave_new)

    # I = interp1d(wave_edge[:-1], np.arange(len(wave)), kind="linear",
    #              bounds_error=False)
    # wave_new_edge_pos = I(wave_new_edge)  # accurate position projected to old
    wave_new_edge_pos = np.interp(wave_new_edge,
                                  wave_edge[:-1], np.arange(len(wave)),
                                  left=np.nan, right=np.nan)
    wave_new_edge_pos2 = np.array(
        [wave_new_edge_pos[:-1], wave_new_edge_pos[1:]]).T  # slipt to lo & hi

    wave_new_ipix = np.floor(wave_new_edge_pos2).astype(int)  # integer part
    wave_new_frac = wave_new_edge_pos2 - wave_new_ipix  # fraction part
    flags = np.any(np.isnan(wave_new_edge_pos2), axis=1)

    result = []

    # rebin flux
    if flux is not None:
        flux = np.asarray(flux)
        assert flux.shape == wave.shape
        flux_new = np.zeros_like(wave_new, dtype=float)
        for ipix, this_flag in enumerate(flags):
            if not this_flag:
                flux_new[ipix] = np.sum(
                    flux[wave_new_ipix[ipix, 0]:wave_new_ipix[ipix, 1]]) \
                    - flux[wave_new_ipix[ipix, 0]] * wave_new_frac[ipix, 0] \
                    + flux[wave_new_ipix[ipix, 1]] * wave_new_frac[ipix, 1]
            else:
                flux_new[ipix] = np.nan
        result.append(flux_new)

    # rebin flux_err
    if flux_err is not None:
        flux_err2 = np.square(np.asarray(flux_err, dtype=float))
        assert flux_err2.shape == wave.shape
        flux_err2_new = np.zeros_like(wave_new, dtype=float)
        for ipix, this_flag in enumerate(flags):
            if not this_flag:
                flux_err2_new[ipix] = np.sum(
                    flux_err2[wave_new_ipix[ipix, 0]:wave_new_ipix[ipix, 1]]) \
                    - flux_err2[wave_new_ipix[ipix, 0]] * wave_new_frac[ipix, 0] \
                    + flux_err2[wave_new_ipix[ipix, 1]] * wave_new_frac[ipix, 1]
            else:
                flux_err2_new[ipix] = np.nan
        result.append(np.sqrt(flux_err2_new))

    # rebin mask
    if mask is not None:
        mask = np.asarray(mask)
        assert mask.shape == wave.shape
        mask_new = np.ones_like(wave_new, dtype=bool)
        for ipix, this_flag in enumerate(flags):
            if not this_flag:
                mask_new[ipix] = np.any(
                    mask[wave_new_ipix[ipix, 0]:wave_new_ipix[ipix, 1] + 1])
        result.append(mask_new)

    if len(result) == 1:
        return result[0]
    elif len(result) > 1:
        return result
    else:
        raise ValueError("@rebin: what to rebin?")


def _rebin_array(data, wave_new_ipix, wave_new_frac, flags):
    """Vectorized rebinning of a 1D or 2D array using precomputed mapping.

    Parameters
    ----------
    data : ndarray, shape (n_old_pix,) or (n_spectra, n_old_pix)
    wave_new_ipix : ndarray, shape (n_new_pix, 2), integer bin edges
    wave_new_frac : ndarray, shape (n_new_pix, 2), fractional parts
    flags : bool ndarray, shape (n_new_pix,), True = out-of-range

    Returns
    -------
    data_new : ndarray, shape (n_new_pix,) or (n_spectra, n_new_pix)
    """
    is_1d = data.ndim == 1
    if is_1d:
        data = data[np.newaxis, :]

    n_spectra = data.shape[0]
    n_new = len(flags)
    good = ~flags
    n_good = np.sum(good)

    if n_good == 0:
        result = np.full((n_spectra, n_new), np.nan)
        return result[0] if is_1d else result

    lo = wave_new_ipix[good, 0]
    hi = wave_new_ipix[good, 1]
    frac_lo = wave_new_frac[good, 0]
    frac_hi = wave_new_frac[good, 1]

    # Cumulative sum trick: cumsum[hi] - cumsum[lo] gives sum of data[lo:hi]
    cumsum = np.zeros((n_spectra, data.shape[1] + 1), dtype=np.float64)
    cumsum[:, 1:] = np.cumsum(data, axis=1)

    sums = cumsum[:, hi] - cumsum[:, lo]  # (n_spectra, n_good)
    # Subtract fractional lo pixel, add fractional hi pixel
    sums -= data[:, lo] * frac_lo[np.newaxis, :]
    sums += data[:, hi] * frac_hi[np.newaxis, :]

    result = np.full((n_spectra, n_new), np.nan)
    result[:, good] = sums

    return result[0] if is_1d else result


def rebin_map(wave, wave_new):
    """Precompute the rebinning map from wave to wave_new.

    Returns (wave_new_ipix, wave_new_frac, flags) that can be reused
    across many spectra sharing the same wavelength grids.
    """
    wave = np.asarray(wave)
    wave_new = np.asarray(wave_new)

    wave_edge = center2edge(wave)
    wave_new_edge = center2edge(wave_new)

    wave_new_edge_pos = np.interp(
        wave_new_edge, wave_edge[:-1], np.arange(len(wave)),
        left=np.nan, right=np.nan
    )
    wave_new_edge_pos2 = np.array(
        [wave_new_edge_pos[:-1], wave_new_edge_pos[1:]]
    ).T

    wave_new_ipix = np.floor(wave_new_edge_pos2).astype(int)
    wave_new_frac = wave_new_edge_pos2 - wave_new_ipix
    flags = np.any(np.isnan(wave_new_edge_pos2), axis=1)

    return wave_new_ipix, wave_new_frac, flags


def rebin_batch(wave_batch, flux_batch, flux_err_batch, wave_new):
    """Rebin a batch of spectra onto a common new wavelength grid.

    Spectra that share the same input wavelength grid reuse their
    rebinning map (the common case for BossCombinedSpectrum).

    Parameters
    ----------
    wave_batch : list of ndarray, each (n_old_pix,)
    flux_batch : list of ndarray, each (n_old_pix,)
    flux_err_batch : list of ndarray, each (n_old_pix,)
    wave_new : ndarray (n_new_pix,)

    Returns
    -------
    flux_new : ndarray (n_spectra, n_new_pix)
    ivar_new : ndarray (n_spectra, n_new_pix)
    """
    wave_new = np.asarray(wave_new)
    n_spectra = len(flux_batch)
    n_new = len(wave_new)

    flux_new = np.full((n_spectra, n_new), np.nan)
    ivar_new = np.full((n_spectra, n_new), np.nan)

    # Group spectra by wavelength grid AND flux length to ensure stackability.
    # Key includes wave(first, last, len) and flux length for robustness.
    groups = {}
    for i in range(n_spectra):
        w = np.asarray(wave_batch[i])
        f = np.asarray(flux_batch[i])
        fe = np.asarray(flux_err_batch[i])
        key = (w[0], w[-1], len(w), len(f), len(fe))
        if key not in groups:
            groups[key] = (w, [])
        groups[key][1].append(i)

    for (wave_proto, indices) in groups.values():
        rmap = rebin_map(wave_proto, wave_new)
        ipix, frac, flags = rmap

        n_grp = len(indices)
        n_wav = len(wave_proto)

        # Pre-allocate and fill to avoid np.array on ragged lists
        flux_stack = np.empty((n_grp, n_wav), dtype=np.float64)
        ferr2_stack = np.empty((n_grp, n_wav), dtype=np.float64)
        for j, idx in enumerate(indices):
            flux_stack[j] = np.asarray(flux_batch[idx], dtype=np.float64)
            ferr2_stack[j] = np.square(
                np.asarray(flux_err_batch[idx], dtype=np.float64)
            )

        flux_rebinned = _rebin_array(flux_stack, ipix, frac, flags)
        ferr2_rebinned = _rebin_array(ferr2_stack, ipix, frac, flags)

        # Convert flux_err^2 -> ivar, guarding against zero/negative
        with np.errstate(divide='ignore', invalid='ignore'):
            ivar_rebinned = np.where(ferr2_rebinned > 0, 1.0 / ferr2_rebinned, 0.0)

        for j, idx in enumerate(indices):
            flux_new[idx] = flux_rebinned[j]
            ivar_new[idx] = ivar_rebinned[j]

    return flux_new, ivar_new


def _test():
    wave, flux, wave_new = np.arange(10), np.ones(10), np.arange(0, 10, 2) + 0.5
    flux[5] += 1
    flux_err = flux
    mask = ~ (flux > 0)
    mask[5] = True
    print("========================")
    print(wave, flux)
    print("========================")
    print(wave, rebin(wave, flux, wave_new=wave_new))
    print("========================")
    print(wave_new, rebin(
        wave, flux=flux, flux_err=flux_err, mask=mask, wave_new=wave_new))
    print("========================")
    # figure()
    # plot(wave, flux, 'x-')
    # plot(wave_new, rebin(wave, flux, wave_new), 's-')
    return


if __name__ == "__main__":
    _test()
