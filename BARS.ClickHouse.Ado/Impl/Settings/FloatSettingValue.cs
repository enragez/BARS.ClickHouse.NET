﻿namespace BARS.ClickHouse.Ado.Impl.Settings
{
    using System;
    using System.Globalization;

    internal class FloatSettingValue : SettingValue
    {
        public FloatSettingValue(float value)
        {
            Value = value;
        }

        public float Value { get; set; }

        protected internal override void Write(ProtocolFormatter formatter)
        {
            formatter.WriteString(Value.ToString(CultureInfo.InvariantCulture));
        }


        internal override T As<T>()
        {
            if (typeof(T) != typeof(float))
            {
                throw new InvalidCastException();
            }

            return (T) (object) Value;
        }

        internal override object AsValue()
        {
            return Value;
        }
    }
}