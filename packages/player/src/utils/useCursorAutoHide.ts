import { useEffect, useState } from "react";

export function useCursorAutoHide(enabled: boolean, delay = 1200) {
	const [hidden, setHidden] = useState(false);

	useEffect(() => {
		if (!enabled) {
			setHidden(false);
			return;
		}

		let timerId: ReturnType<typeof setTimeout>;
		const bump = () => {
			clearTimeout(timerId);
			setHidden(false);
			timerId = setTimeout(() => setHidden(true), delay);
		};
		bump();

		window.addEventListener("mousemove", bump);
		window.addEventListener("mousedown", bump);
		return () => {
			clearTimeout(timerId);
			window.removeEventListener("mousemove", bump);
			window.removeEventListener("mousedown", bump);
			setHidden(false);
		};
	}, [enabled, delay]);

	return hidden;
}
