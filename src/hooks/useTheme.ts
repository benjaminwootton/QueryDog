import { useState, useEffect } from 'react';
import { themeAlpine } from 'ag-grid-community';

const gridFontFamily = '"JetBrains Mono", ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace';

const darkGridTheme = themeAlpine.withParams({
  backgroundColor: '#111827',
  headerBackgroundColor: '#1f2937',
  oddRowBackgroundColor: '#111827',
  rowHoverColor: '#1f2937',
  borderColor: '#374151',
  foregroundColor: '#9ca3af',
  headerTextColor: '#f3f4f6',
  fontFamily: gridFontFamily,
  fontSize: 9,
  headerFontSize: 11,
  headerFontWeight: 600,
  cellTextColor: '#9ca3af',
  rowHeight: 26,
  headerHeight: 30,
});

const lightGridTheme = themeAlpine.withParams({
  backgroundColor: '#ffffff',
  headerBackgroundColor: '#f9fafb',
  oddRowBackgroundColor: '#ffffff',
  rowHoverColor: '#f3f4f6',
  borderColor: '#e5e7eb',
  foregroundColor: '#1f2937',
  headerTextColor: '#030712',
  fontFamily: gridFontFamily,
  fontSize: 9,
  headerFontSize: 11,
  headerFontWeight: 600,
  cellTextColor: '#1f2937',
  rowHeight: 26,
  headerHeight: 30,
});

export function useIsLightMode() {
  const [isLight, setIsLight] = useState(() => document.documentElement.classList.contains('light'));
  useEffect(() => {
    const observer = new MutationObserver(() => {
      setIsLight(document.documentElement.classList.contains('light'));
    });
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ['class'] });
    return () => observer.disconnect();
  }, []);
  return isLight;
}

export function useGridTheme() {
  const isLight = useIsLightMode();
  return isLight ? lightGridTheme : darkGridTheme;
}
