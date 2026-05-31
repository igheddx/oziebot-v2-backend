export function routeForProductKey(productKey: string | null | undefined): string {
  if (productKey === "teacher_assist") return "/teacher-assist-v2";
  return "/dashboard";
}

export function productKeyForPathname(pathname: string): string | null {
  if (pathname.startsWith("/teacher-assist-v2") || pathname.startsWith("/teacher-assist")) {
    return "teacher_assist";
  }
  if (pathname === "/" || pathname === "/login" || pathname.startsWith("/admin")) return null;
  return "trading";
}
