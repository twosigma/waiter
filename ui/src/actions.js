const Actions = {
  SET_THEME: 'SET_THEME',
};

const toggleTheme = () => (dispatch, getState) => (
  dispatch({
    type: Actions.SET_THEME,
    theme: getState().getIn(['settings', 'theme']) === 'light' ? 'dark' : 'light',
  })
);

export {
  Actions,
  toggleTheme,
};
