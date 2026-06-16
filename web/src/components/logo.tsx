import Image from 'next/image';

export function Logo({ size = 28 }: { size?: number }) {
  return (
    <div className="flex items-center gap-2.5">
      <Image
        src="/logo.png"
        alt="Interchained"
        width={size}
        height={size}
        className="rounded-full"
        style={{ width: size, height: size }}
        priority
        unoptimized
      />
      <div className="leading-tight">
        <div className="text-sm font-semibold tracking-tight">Interchained</div>
        <div className="text-[10px] uppercase tracking-[0.2em] text-[var(--color-text-faint)]">Vision</div>
      </div>
    </div>
  );
}
