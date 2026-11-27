import numpy as np
from scipy.interpolate import bisplev


def load_pca_model(path):
    """
    Load a PCA spectral model from a `.npz` file.

    :param path:
        The path to the `.npz` file written by `export_pca_model`.

    :returns:
        A dictionary of model arrays.
    """
    with np.load(path, allow_pickle=False) as data:
        model = {key: data[key] for key in data.files}
    model["kx"] = int(model["kx"])
    model["ky"] = int(model["ky"])
    return model


def reconstruct_spectrum(teff, logg, pca_model):
    """
    Reconstruct a model spectrum at the given effective temperature and surface gravity.

    :param teff:
        The effective temperature [K].

    :param logg:
        The surface gravity, in units of 100 * log10(g).

    :param pca_model:
        The model dictionary returned by `load_pca_model`.

    :returns:
        The model flux, sampled at `pca_model["wavelength"]`.
    """
    tx, ty = pca_model["tx"], pca_model["ty"]
    kx, ky = pca_model["kx"], pca_model["ky"]
    c = np.array([bisplev(teff, logg, (tx, ty, coeffs, kx, ky)) for coeffs in pca_model["coeffs"]])
    log_flux = pca_model["mean"] + c @ pca_model["components"]
    return np.exp(log_flux)


def export_pca_model(pickle_path, npz_path, n_check=1000, seed=0):
    """
    Convert a pickled PCA spectral model to a `.npz` file, and check that it reconstructs identical spectra.

    :param pickle_path:
        The path to the pickled model.

    :param npz_path:
        The path to write the `.npz` file.

    :param n_check: [optional]
        The number of random (teff, logg) points to compare after export (default: 1000).

    :param seed: [optional]
        The random seed for the comparison points (default: 0).

    :returns:
        The maximum absolute difference between the pickled and exported models.
    """
    import pickle

    with open(pickle_path, "rb") as fp:
        original = pickle.load(fp)

    pca = original["pca"]
    if getattr(pca, "whiten", False):
        raise ValueError("Whitened PCA models are not supported")

    splines = original["interp_coeffs"]
    if len(splines) != pca.n_components_:
        raise ValueError(f"Expected {pca.n_components_} splines, got {len(splines)}")

    tx, ty = splines[0].get_knots()
    kx, ky = splines[0].degrees
    for k, spline in enumerate(splines):
        stx, sty = spline.get_knots()
        if not (np.array_equal(stx, tx) and np.array_equal(sty, ty) and tuple(spline.degrees) == (kx, ky)):
            raise ValueError(f"Spline {k} does not share knots and degrees with spline 0")

    np.savez(
        npz_path,
        mean=np.asarray(pca.mean_, dtype=float),
        components=np.asarray(pca.components_, dtype=float),
        tx=np.asarray(tx, dtype=float),
        ty=np.asarray(ty, dtype=float),
        coeffs=np.array([spline.get_coeffs() for spline in splines], dtype=float),
        kx=np.int64(kx),
        ky=np.int64(ky),
        wavelength=np.asarray(original["wavelength"], dtype=float),
        teff_grid=np.asarray(original["teff_grid"], dtype=float),
        logg_grid=np.asarray(original["logg_grid"], dtype=float),
        eps=np.float64(original.get("eps", np.nan)),
    )

    exported = load_pca_model(npz_path)

    rng = np.random.default_rng(seed)
    teffs = rng.uniform(np.min(original["teff_grid"]), np.max(original["teff_grid"]), n_check)
    loggs = rng.uniform(np.min(original["logg_grid"]), np.max(original["logg_grid"]), n_check)
    max_diff = 0.0
    for teff, logg in zip(teffs, loggs):
        c = np.array([spline(teff, logg)[0, 0] for spline in splines])
        expected = np.exp(pca.mean_ + np.dot(c, pca.components_))
        max_diff = max(max_diff, np.max(np.abs(reconstruct_spectrum(teff, logg, exported) - expected)))

    if max_diff != 0:
        raise RuntimeError(f"Exported model differs from pickled model (max absolute difference {max_diff})")
    return max_diff


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        sys.exit("usage: python -m astra.pipelines.snow_white.pca_model INPUT.pkl OUTPUT.npz")
    export_pca_model(sys.argv[1], sys.argv[2])
    print(f"Wrote {sys.argv[2]}")
