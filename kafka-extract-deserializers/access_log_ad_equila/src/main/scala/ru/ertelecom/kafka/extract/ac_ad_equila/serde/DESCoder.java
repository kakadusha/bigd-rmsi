package ru.ertelecom.kafka.extract.ac_ad_equila.serde;


import java.io.UnsupportedEncodingException;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.bouncycastle.crypto.DataLengthException;
import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.crypto.engines.DESEngine;
import org.bouncycastle.crypto.modes.CBCBlockCipher;
import org.bouncycastle.crypto.paddings.PaddedBufferedBlockCipher;
import org.bouncycastle.crypto.paddings.ZeroBytePadding;
import org.bouncycastle.crypto.params.KeyParameter;

public class DESCoder {
    /**
     *
     * @param key - DES key (8 bytes)
     * @param value - string to be encoded
     * @return returns string of hex symbols representing encoded byte array
     */
    public static String encode(String key, String value)
            throws DataLengthException, IllegalStateException,
            InvalidCipherTextException, UnsupportedEncodingException {
        byte[] data = value.getBytes("UTF-8");

        PaddedBufferedBlockCipher cipher = new PaddedBufferedBlockCipher(
                new CBCBlockCipher(new DESEngine()), new ZeroBytePadding());
        cipher.init(true, new KeyParameter(key.getBytes("UTF-8")));

        byte[] encryptedData = new byte[cipher.getOutputSize(data.length)];
        int noOfBytes = cipher.processBytes(data, 0, data.length,
                encryptedData, 0);
        noOfBytes += cipher.doFinal(encryptedData, noOfBytes);

        if (data.length % 8 == 0 && encryptedData.length > data.length) {
            byte[] res = new byte[data.length];
            System.arraycopy(encryptedData, 0, res, 0, res.length);
            encryptedData = res;
        }

        return new String(Hex.encodeHex(encryptedData));
    }

    /**
     *
     * @param key - DES key (8 bytes)
     * @param value - string with hex symbols
     * @return returns decoded string
     */
    public static String decode(String key, String value)
            throws DataLengthException, IllegalStateException,
            InvalidCipherTextException, UnsupportedEncodingException, DecoderException {
        return decode(key, Hex.decodeHex(value.toCharArray()));
    }

    protected static String decode(String key, byte[] data)
            throws DataLengthException, IllegalStateException,
            InvalidCipherTextException, UnsupportedEncodingException {
        PaddedBufferedBlockCipher cipher = new PaddedBufferedBlockCipher(
                new CBCBlockCipher(new DESEngine()), new ZeroBytePadding());
        cipher.init(false, new KeyParameter(key.getBytes("UTF-8")));

        byte[] encryptedData = new byte[cipher.getOutputSize(data.length)];
        int noOfBytes = cipher.processBytes(data, 0, data.length,
                encryptedData, 0);
        noOfBytes += cipher.doFinal(encryptedData, noOfBytes);

        byte[] res = new byte[noOfBytes];
        System.arraycopy(encryptedData, 0, res, 0, res.length);
        return new String(res);
    }

}

